import glob, os
from dateutil.parser import parse
from omegaconf import DictConfig

def get_list_filenames(data_path: str="./", ext: str="*", start_str: str = ""):
    """
    Loads a list of file names within a directory.

    Args:
        data_path (str, optional): The directory path to search for files. Defaults to "./".
        ext (str, optional): The file extension to filter the search. Defaults to "*".

    Returns:
        List[str]: A sorted list of file names matching the given extension within the directory.
    """
    pattern = f"*{ext}"
    files = sorted(glob.glob(os.path.join(data_path, "**", pattern), recursive=True))

    if start_str:
        files = [f for f in files if os.path.basename(f).startswith(start_str)]

    return files

def get_files(datasets_spec: DictConfig, ext=".nc"):
    """
    Get a list of filenames based on the provided datasets specification.

    Args:
        datasets_spec (DictConfig): The datasets specification containing the path and extension.
        ext (str, optional): The file extension to filter the search. Defaults to ".nc".

    Returns:
        List[str]: A list of filenames.

    """
    data_path = datasets_spec.data_path
    return get_list_filenames(data_path=data_path, ext=ext)

def get_list_filenames_af(data_path: str="./", ext: str="*", start_str: str = ""):
    """
    Loads a list of file names within a directory, filtering by extension and an optional starting string.

    Args:
        data_path (str, optional): The directory path to search for files. Defaults to "./".
        ext (str, optional): The file extension to filter the search. Defaults to "*".
        start_str (str, optional): The starting string of the filename. Defaults to "", meaning no filtering by prefix.

    Returns:
        List[str]: A sorted list of file names matching the given extension and start string.
    """
    pattern = os.path.join(data_path, "**", f"{start_str}*{ext}")
    return sorted(glob.glob(pattern, recursive=True))
