# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import abc
import shutil
import traceback
from pathlib import Path
from typing import Iterable, List, Union
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

import fire
import numpy as np
import pandas as pd
from tqdm import tqdm
from loguru import logger


def code_to_fname(code: str):
    """stock code to file name

    Parameters
    ----------
    code: str
    """
    # NOTE: In windows, the following name is I/O device, and the file with the corresponding name cannot be created
    # reference: https://superuser.com/questions/86999/why-cant-i-name-a-folder-or-file-con-in-windows
    replace_names = ["CON", "PRN", "AUX", "NUL"]
    replace_names += [f"COM{i}" for i in range(10)]
    replace_names += [f"LPT{i}" for i in range(10)]

    # fallback prefix used when a code matches a reserved filename
    prefix = "_bin_"
    if str(code).upper() in replace_names:
        code = prefix + str(code)

    return code

def fname_to_code(fname: str):
    """file name to stock code

    Parameters
    ----------
    fname: str
    """

    prefix = "_bin_"
    if fname.startswith(prefix):
        fname = fname.lstrip(prefix)
    return fname


def read_as_df(file_path: Union[str, Path], store=None, **kwargs) -> pd.DataFrame:
    """
    Read a csv or parquet file into a pandas DataFrame.

    Parameters
    ----------
    file_path : Union[str, Path]
        Path to the data file.
    store : Option storage backend.
    **kwargs :
        Additional keyword arguments passed to the underlying pandas
        reader.

    Returns
    -------
    pd.DataFrame
    """
    from .storage import get_storage, FileSystemStore
    if store is None:
        store = get_storage("fs")

    # stringify to deal with non-fs paths uniformly
    file_str = str(file_path)
    suffix = pathlib_suffix(file_str)

    keep_keys = {".csv": ("low_memory",)}
    kept_kwargs = {}
    for k in keep_keys.get(suffix, []):
        if k in kwargs:
            kept_kwargs[k] = kwargs[k]

    if suffix == ".csv":
        import io
        if isinstance(store, FileSystemStore):
            df = pd.read_csv(file_path, **kept_kwargs)
        else:
            df = pd.read_csv(io.BytesIO(store.read_bytes(file_str)), **kept_kwargs)
        for col in df.select_dtypes(include=["string"]):
            df[col] = df[col].astype("object")
        return df
    elif suffix == ".parquet":
        if isinstance(store, FileSystemStore):
            return pd.read_parquet(file_path, **kept_kwargs)
        else:
            import io
            return pd.read_parquet(io.BytesIO(store.read_bytes(file_str)), **kept_kwargs)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")

def pathlib_suffix(p: str) -> str:
    p = str(p)
    if "." in p.split("/")[-1]:
        return "." + p.split("/")[-1].split(".")[-1].lower()
    return ""


class DumpDataBase:
    INSTRUMENTS_START_FIELD = "start_datetime"
    INSTRUMENTS_END_FIELD = "end_datetime"
    CALENDARS_DIR_NAME = "calendars"
    FEATURES_DIR_NAME = "features"
    INSTRUMENTS_DIR_NAME = "instruments"
    DUMP_FILE_SUFFIX = ".bin"
    DAILY_FORMAT = "%Y-%m-%d"
    HIGH_FREQ_FORMAT = "%Y-%m-%d %H:%M:%S"
    INSTRUMENTS_SEP = "\t"
    INSTRUMENTS_FILE_NAME = "all.txt"

    UPDATE_MODE = "update"
    ALL_MODE = "all"

    def __init__(
        self,
        data_path: str,
        dump_dir: str,
        backup_dir: str = None,
        freq: str = "day",
        max_workers: int = 16,
        date_field_name: str = "date",
        file_suffix: str = ".csv",
        symbol_field_name: str = "symbol",
        exclude_fields: str = "",
        include_fields: str = "",
        limit_nums: int = None,
        store_type: str = "fs",
    ):
        """

        Parameters
        ----------
        data_path: str
            stock data path or directory
        dump_dir: str
            target directory for generated binary files
        backup_dir: str, default None
            if backup_dir is not None, backup dump_dir to backup_dir
        freq: str, default "day"
            transaction frequency
        max_workers: int, default None
            number of threads
        date_field_name: str, default "date"
            the name of the date field in the csv
        file_suffix: str, default ".csv"
            file suffix
        symbol_field_name: str, default "symbol"
            symbol field name
        include_fields: tuple
            dump fields
        exclude_fields: tuple
            fields not dumped
        limit_nums: int
            Use when debugging, default None
        store_type: str
            the storage backend type
        """
        from .storage import get_storage
        self.store = get_storage(store_type, dump_dir)
        self.store_type = store_type

        # data_path might be bucket/prefix in GCS
        if store_type == "fs":
            data_path_obj = Path(data_path).expanduser()
            if data_path_obj.is_dir():
                self.df_files = sorted([str(p) for p in data_path_obj.glob(f"*{file_suffix}")])
            else:
                self.df_files = [str(data_path_obj)]
        else:
            self.df_files = sorted(self.store.glob(data_path, f"*{file_suffix}"))
            # eliminate any entries that don't actually end in the expected suffix
            self.df_files = [f for f in self.df_files if pathlib_suffix(f) == file_suffix]
            if not self.df_files:
                # if the user provided a direct file path matching suffix, honor it;
                # otherwise treat this as an error since no matching files were found.
                if pathlib_suffix(data_path) == file_suffix:
                    self.df_files = [data_path]
                else:
                    raise FileNotFoundError(
                        f"no files with suffix '{file_suffix}' found under '{data_path}' on store '{store_type}'"
                    )
        
        if isinstance(exclude_fields, str):
            exclude_fields = exclude_fields.split(",")
        if isinstance(include_fields, str):
            include_fields = include_fields.split(",")
        self._exclude_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, exclude_fields)))
        self._include_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, include_fields)))
        self.file_suffix = file_suffix
        self.symbol_field_name = symbol_field_name

        if limit_nums is not None:
            self.df_files = self.df_files[: int(limit_nums)]
            
        self.dump_dir = str(Path(dump_dir).expanduser()) if store_type == "fs" else dump_dir
        
        if backup_dir is not None:
            self.backup_dir = str(Path(backup_dir).expanduser()) if store_type == "fs" else backup_dir
            self._backup_dir(self.backup_dir)
        else:
            self.backup_dir = None

        self.freq = freq
        self.calendar_format = self.DAILY_FORMAT if self.freq == "day" else self.HIGH_FREQ_FORMAT

        self.works = max_workers
        self.date_field_name = date_field_name

        self._calendars_dir = self.store.joinpath(self.dump_dir, self.CALENDARS_DIR_NAME)
        self._features_dir = self.store.joinpath(self.dump_dir, self.FEATURES_DIR_NAME)
        self._instruments_dir = self.store.joinpath(self.dump_dir, self.INSTRUMENTS_DIR_NAME)

        self._calendars_list = []

        self._mode = self.ALL_MODE
        self._kwargs = {}

    def _backup_dir(self, target_dir: str):
        if self.store_type == "fs":
            shutil.copytree(str(Path(self.dump_dir).resolve()), str(Path(target_dir).resolve()))
        else:
            logger.warning("backup_dir is not yet fully supported for non-fs storage")

    def _format_datetime(self, datetime_d: [str, pd.Timestamp]):
        datetime_d = pd.Timestamp(datetime_d)
        return datetime_d.strftime(self.calendar_format)

    def _get_date(
        self, file_or_df: [str, pd.DataFrame], *, is_begin_end: bool = False, as_set: bool = False
    ) -> Iterable[pd.Timestamp]:
        # guard against invalid file paths that slipped through
        if isinstance(file_or_df, str) and not file_or_df:
            # empty string, nothing to read
            if is_begin_end and as_set:
                return (None, None), set()
            if is_begin_end:
                return None, None
            if as_set:
                return set()
            return []
        if not isinstance(file_or_df, pd.DataFrame):
            df = self._get_source_data(file_or_df)
        else:
            df = file_or_df
        if df.empty or self.date_field_name not in df.columns.tolist():
            _calendars = pd.Series(dtype=np.float32)
        else:
            _calendars = df[self.date_field_name]

        if is_begin_end and as_set:
            return (_calendars.min(), _calendars.max()), set(_calendars)
        elif is_begin_end:
            return _calendars.min(), _calendars.max()
        elif as_set:
            return set(_calendars)
        else:
            return _calendars.tolist()

    def _get_source_data(self, file_path: str) -> pd.DataFrame:
        df = read_as_df(file_path, store=self.store, low_memory=False)
        if self.date_field_name in df.columns:
            df[self.date_field_name] = pd.to_datetime(df[self.date_field_name])
        # df.drop_duplicates([self.date_field_name], inplace=True)
        return df

    def get_symbol_from_file(self, file_path: str) -> str:
        # file_path is string
        stem = str(file_path).split("/")[-1].split(".")[0]
        return fname_to_code(stem.strip().lower())

    def get_dump_fields(self, df_columns: Iterable[str]) -> Iterable[str]:
        return (
            self._include_fields
            if self._include_fields
            else set(df_columns) - set(self._exclude_fields) if self._exclude_fields else df_columns
        )

    def _read_calendars(self, calendar_path: str) -> List[pd.Timestamp]:
        import io
        if self.store_type == "fs":
            df = pd.read_csv(calendar_path, header=None)
        else:
            df = pd.read_csv(io.BytesIO(self.store.read_bytes(calendar_path)), header=None)
            
        return sorted(
            map(
                pd.Timestamp,
                df.loc[:, 0].tolist(),
            )
        )

    def _read_instruments(self, instrument_path: str) -> pd.DataFrame:
        import io
        if self.store_type == "fs":
            df = pd.read_csv(
                instrument_path,
                sep=self.INSTRUMENTS_SEP,
                names=[
                    self.symbol_field_name,
                    self.INSTRUMENTS_START_FIELD,
                    self.INSTRUMENTS_END_FIELD,
                ],
            )
        else:
            df = pd.read_csv(
                io.BytesIO(self.store.read_bytes(instrument_path)),
                sep=self.INSTRUMENTS_SEP,
                names=[
                    self.symbol_field_name,
                    self.INSTRUMENTS_START_FIELD,
                    self.INSTRUMENTS_END_FIELD,
                ],
            )

        return df

    def save_calendars(self, calendars_data: list):
        self.store.mkdir(self._calendars_dir, parents=True, exist_ok=True)
        calendars_path = self.store.joinpath(self._calendars_dir, f"{self.freq}.txt")
        result_calendars_list = [self._format_datetime(x) for x in calendars_data]
        if self.store_type == "fs":
            np.savetxt(calendars_path, result_calendars_list, fmt="%s", encoding="utf-8")
        else:
            import io
            bio = io.BytesIO()
            np.savetxt(bio, result_calendars_list, fmt="%s", encoding="utf-8")
            self.store.write_bytes(calendars_path, bio.getvalue())

    def save_instruments(self, instruments_data: Union[list, pd.DataFrame]):
        self.store.mkdir(self._instruments_dir, parents=True, exist_ok=True)
        instruments_path = self.store.joinpath(self._instruments_dir, self.INSTRUMENTS_FILE_NAME)
        import io
        if isinstance(instruments_data, pd.DataFrame):
            _df_fields = [self.symbol_field_name, self.INSTRUMENTS_START_FIELD, self.INSTRUMENTS_END_FIELD]
            instruments_data = instruments_data.loc[:, _df_fields]
            instruments_data[self.symbol_field_name] = instruments_data[self.symbol_field_name].apply(
                lambda x: fname_to_code(x.lower()).upper()
            )
            if self.store_type == "fs":
                instruments_data.to_csv(instruments_path, header=False, sep=self.INSTRUMENTS_SEP, index=False)
            else:
                bio = io.BytesIO()
                instruments_data.to_csv(bio, header=False, sep=self.INSTRUMENTS_SEP, index=False)
                self.store.write_bytes(instruments_path, bio.getvalue())
        else:
            if self.store_type == "fs":
                np.savetxt(instruments_path, instruments_data, fmt="%s", encoding="utf-8")
            else:
                bio = io.BytesIO()
                np.savetxt(bio, instruments_data, fmt="%s", encoding="utf-8")
                self.store.write_bytes(instruments_path, bio.getvalue())

    def data_merge_calendar(self, df: pd.DataFrame, calendars_list: List[pd.Timestamp]) -> pd.DataFrame:
        # calendars
        calendars_df = pd.DataFrame(data=calendars_list, columns=[self.date_field_name])
        calendars_df[self.date_field_name] = calendars_df[self.date_field_name].astype("datetime64[ns]")
        cal_df = calendars_df[
            (calendars_df[self.date_field_name] >= df[self.date_field_name].min())
            & (calendars_df[self.date_field_name] <= df[self.date_field_name].max())
        ]
        # align index
        cal_df.set_index(self.date_field_name, inplace=True)
        df.set_index(self.date_field_name, inplace=True)
        r_df = df.reindex(cal_df.index)
        return r_df

    @staticmethod
    def get_datetime_index(df: pd.DataFrame, calendar_list: List[pd.Timestamp]) -> int:
        return calendar_list.index(df.index.min())

    def _data_to_bin(self, df: pd.DataFrame, calendar_list: List[pd.Timestamp], features_dir: str, code: str):
        if df.empty:
            logger.warning(f"{code} data is None or empty")
            return
        if not calendar_list:
            logger.warning("calendar_list is empty")
            return
        # align index
        _df = self.data_merge_calendar(df, calendar_list)
        if _df.empty:
            logger.warning(f"{code} data is not in calendars")
            return
        # used when creating a bin file
        date_index = self.get_datetime_index(_df, calendar_list)
        for field in self.get_dump_fields(_df.columns):
            bin_path = self.store.joinpath(features_dir, f"{field.lower()}.{self.freq}{self.DUMP_FILE_SUFFIX}")
            if field not in _df.columns:
                continue
            if self.store.exists(bin_path) and self._mode == self.UPDATE_MODE:
                # update
                if self.store_type == "fs":
                    with Path(bin_path).open("ab") as fp:
                        np.array(_df[field]).astype("<f").tofile(fp)
                else:
                    self.store.append_bytes(bin_path, np.array(_df[field]).astype("<f").tobytes())
                logger.info(f"updated bin file: {bin_path} (symbol={code}, field={field})")
            else:
                # append; self._mode == self.ALL_MODE or not bin_path.exists()
                if self.store_type == "fs":
                    np.hstack([date_index, _df[field]]).astype("<f").tofile(str(Path(bin_path).resolve()))
                else:
                    self.store.write_bytes(bin_path, np.hstack([date_index, _df[field]]).astype("<f").tobytes())
                logger.info(f"created/overwritten bin file: {bin_path} (symbol={code}, field={field})")

    def _dump_bin(self, file_or_data: [str, pd.DataFrame], calendar_list: List[pd.Timestamp]):
        if not calendar_list:
            logger.warning("calendar_list is empty")
            return
        if isinstance(file_or_data, pd.DataFrame):
            if file_or_data.empty:
                return
            code = fname_to_code(str(file_or_data.iloc[0][self.symbol_field_name]).lower())
            df = file_or_data
        elif isinstance(file_or_data, (str, Path)):
            code = self.get_symbol_from_file(file_or_data)
            df = self._get_source_data(file_or_data)
        else:
            raise ValueError(f"not support {type(file_or_data)}")
        if df is None or df.empty:
            logger.warning(f"{code} data is None or empty")
            return

        # try to remove dup rows or it will cause exception when reindex.
        df = df.drop_duplicates(self.date_field_name)

        # features save dir
        features_dir = self.store.joinpath(self._features_dir, code_to_fname(code).lower())
        self.store.mkdir(features_dir, parents=True, exist_ok=True)
        self._data_to_bin(df, calendar_list, features_dir, code)

    @abc.abstractmethod
    def dump(self):
        raise NotImplementedError("dump not implemented!")

    def __call__(self, *args, **kwargs):
        self.dump()


class DumpDataAll(DumpDataBase):
    def _get_all_date(self):
        logger.info("start get all date......")
        all_datetime = set()
        date_range_list = []
        _fun = partial(self._get_date, as_set=True, is_begin_end=True)
        executor_class = ProcessPoolExecutor if self.store_type == "fs" else ThreadPoolExecutor
        with tqdm(total=len(self.df_files)) as p_bar:
            with executor_class(max_workers=self.works) as executor:
                for file_path, ((_begin_time, _end_time), _set_calendars) in zip(
                    self.df_files, executor.map(_fun, self.df_files)
                ):
                    all_datetime = all_datetime | _set_calendars
                    if isinstance(_begin_time, pd.Timestamp) and isinstance(_end_time, pd.Timestamp):
                        _begin_time = self._format_datetime(_begin_time)
                        _end_time = self._format_datetime(_end_time)
                        symbol = self.get_symbol_from_file(file_path)
                        _inst_fields = [symbol.upper(), _begin_time, _end_time]
                        date_range_list.append(f"{self.INSTRUMENTS_SEP.join(_inst_fields)}")
                    p_bar.update()
        self._kwargs["all_datetime_set"] = all_datetime
        self._kwargs["date_range_list"] = date_range_list
        logger.info("end of get all date.\n")

    def _dump_calendars(self):
        logger.info("start dump calendars......")
        self._calendars_list = sorted(map(pd.Timestamp, self._kwargs["all_datetime_set"]))
        self.save_calendars(self._calendars_list)
        logger.info("end of calendars dump.\n")

    def _dump_instruments(self):
        logger.info("start dump instruments......")
        self.save_instruments(self._kwargs["date_range_list"])
        logger.info("end of instruments dump.\n")

    def _dump_features(self):
        logger.info("start dump features......")
        _dump_func = partial(self._dump_bin, calendar_list=self._calendars_list)
        executor_class = ProcessPoolExecutor if self.store_type == "fs" else ThreadPoolExecutor
        with tqdm(total=len(self.df_files)) as p_bar:
            with executor_class(max_workers=self.works) as executor:
                for _ in executor.map(_dump_func, self.df_files):
                    p_bar.update()

        logger.info("end of features dump.\n")

    def dump(self):
        self._get_all_date()
        self._dump_calendars()
        self._dump_instruments()
        self._dump_features()


class DumpDataFix(DumpDataAll):
    def _dump_instruments(self):
        logger.info("start dump instruments......")
        _fun = partial(self._get_date, is_begin_end=True)
        new_stock_files = sorted(
            filter(
                lambda x: self.get_symbol_from_file(x).upper() not in self._old_instruments,
                self.df_files,
            )
        )
        with tqdm(total=len(new_stock_files)) as p_bar:
            with ProcessPoolExecutor(max_workers=self.works) as execute:
                for file_path, (_begin_time, _end_time) in zip(new_stock_files, execute.map(_fun, new_stock_files)):
                    if isinstance(_begin_time, pd.Timestamp) and isinstance(_end_time, pd.Timestamp):
                        symbol = self.get_symbol_from_file(file_path).upper()
                        _dt_map = self._old_instruments.setdefault(symbol, dict())
                        _dt_map[self.INSTRUMENTS_START_FIELD] = self._format_datetime(_begin_time)
                        _dt_map[self.INSTRUMENTS_END_FIELD] = self._format_datetime(_end_time)
                    p_bar.update()
        _inst_df = pd.DataFrame.from_dict(self._old_instruments, orient="index")
        _inst_df.index.names = [self.symbol_field_name]
        self.save_instruments(_inst_df.reset_index())
        logger.info("end of instruments dump.\n")

    def dump(self):
        cal_path = self.store.joinpath(self._calendars_dir, f"{self.freq}.txt")
        self._calendars_list = self._read_calendars(cal_path)
        # noinspection PyAttributeOutsideInit
        inst_path = self.store.joinpath(self._instruments_dir, self.INSTRUMENTS_FILE_NAME)
        self._old_instruments = (
            self._read_instruments(inst_path)
            .set_index([self.symbol_field_name])
            .to_dict(orient="index")
        )  # type: dict
        self._dump_instruments()
        self._dump_features()


class DumpDataUpdate(DumpDataBase):
    def __init__(
        self,
        data_path: str,
        dump_dir: str,
        backup_dir: str = None,
        freq: str = "day",
        max_workers: int = 16,
        date_field_name: str = "date",
        file_suffix: str = ".csv",
        symbol_field_name: str = "symbol",
        exclude_fields: str = "",
        include_fields: str = "",
        limit_nums: int = None,
        store_type: str = "fs",
    ):
        super().__init__(
            data_path,
            dump_dir,
            backup_dir,
            freq,
            max_workers,
            date_field_name,
            file_suffix,
            symbol_field_name,
            exclude_fields,
            include_fields,
            limit_nums,
            store_type=store_type,
        )
        self._mode = self.UPDATE_MODE
        cal_path = self.store.joinpath(self._calendars_dir, f"{self.freq}.txt")
        self._old_calendar_list = self._read_calendars(cal_path)
        # NOTE: all.txt only exists once for each stock
        # NOTE: if a stock corresponds to multiple different time ranges, user need to modify self._update_instruments
        inst_path = self.store.joinpath(self._instruments_dir, self.INSTRUMENTS_FILE_NAME)
        self._update_instruments = (
            self._read_instruments(inst_path)
            .set_index([self.symbol_field_name])
            .to_dict(orient="index")
        )  # type: dict

        # load all csv files
        self._all_data = self._load_all_source_data()  # type: pd.DataFrame
        self._new_calendar_list = self._old_calendar_list + sorted(
            filter(lambda x: x > self._old_calendar_list[-1], self._all_data[self.date_field_name].unique())
        )

    def _load_all_source_data(self):
        # NOTE: Need more memory
        logger.info("start load all source data....")
        all_df = []

        def _read_df(file_path: Path):
            _df = read_as_df(file_path)
            if self.date_field_name in _df.columns and not np.issubdtype(
                _df[self.date_field_name].dtype, np.datetime64
            ):
                _df[self.date_field_name] = pd.to_datetime(_df[self.date_field_name])
            if self.symbol_field_name not in _df.columns:
                _df[self.symbol_field_name] = self.get_symbol_from_file(file_path)
            return _df

        with tqdm(total=len(self.df_files)) as p_bar:
            with ThreadPoolExecutor(max_workers=self.works) as executor:
                for df in executor.map(_read_df, self.df_files):
                    if not df.empty:
                        all_df.append(df)
                    p_bar.update()

        logger.info("end of load all data.\n")
        return pd.concat(all_df, sort=False)

    def _dump_calendars(self):
        pass

    def _dump_instruments(self):
        pass

    def _dump_features(self):
        logger.info("start dump features......")
        error_code = {}
        with ProcessPoolExecutor(max_workers=self.works) as executor:
            futures = {}
            for _code, _df in self._all_data.groupby(self.symbol_field_name, group_keys=False):
                _code = fname_to_code(str(_code).lower()).upper()
                _start, _end = self._get_date(_df, is_begin_end=True)
                if not (isinstance(_start, pd.Timestamp) and isinstance(_end, pd.Timestamp)):
                    continue
                if _code in self._update_instruments:
                    # exists stock, will append data
                    _update_calendars = (
                        _df[_df[self.date_field_name] > self._update_instruments[_code][self.INSTRUMENTS_END_FIELD]][
                            self.date_field_name
                        ]
                        .sort_values()
                        .to_list()
                    )
                    if _update_calendars:
                        self._update_instruments[_code][self.INSTRUMENTS_END_FIELD] = self._format_datetime(_end)
                        futures[executor.submit(self._dump_bin, _df, _update_calendars)] = _code
                else:
                    # new stock
                    _dt_range = self._update_instruments.setdefault(_code, dict())
                    _dt_range[self.INSTRUMENTS_START_FIELD] = self._format_datetime(_start)
                    _dt_range[self.INSTRUMENTS_END_FIELD] = self._format_datetime(_end)
                    futures[executor.submit(self._dump_bin, _df, self._new_calendar_list)] = _code

            with tqdm(total=len(futures)) as p_bar:
                for _future in as_completed(futures):
                    try:
                        _future.result()
                    except Exception:
                        error_code[futures[_future]] = traceback.format_exc()
                    p_bar.update()
            logger.info(f"dump bin errors: {error_code}")

        logger.info("end of features dump.\n")

    def dump(self):
        self.save_calendars(self._new_calendar_list)
        self._dump_features()
        df = pd.DataFrame.from_dict(self._update_instruments, orient="index")
        df.index.names = [self.symbol_field_name]
        self.save_instruments(df.reset_index())


if __name__ == "__main__":
    fire.Fire({"dump_all": DumpDataAll, "dump_fix": DumpDataFix, "dump_update": DumpDataUpdate})
