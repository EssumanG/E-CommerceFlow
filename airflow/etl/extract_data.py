import os
import kagglehub
import shutil


def is_path_exist(path: str) -> bool:
    return os.path.exists(path)

def create_directory(path: str) -> None:
    #check if path exist
    if is_path_exist(path):
        print(f"The path {path} already exist")
        return
    os.mkdir(path)

def download_data(url: str, dest_path: str) -> None:
    """
    Download the data from kaggle store is the destination path dir
    """
    if not is_path_exist(dest_path):
        print(f"The path {dest_path} does not exist")
        return
    else:
        # Download latest version
        path = kagglehub.dataset_download(url)
        print("Dataset downloaded succesfully")
        print("Path to dataset files:", dest_path)
        file_names = os.listdir(path)
            
        for file_name in file_names:
            shutil.move(os.path.join(path, file_name), dest_path)
        shutil.rmtree(path)




# if "__main__" == __name__:
#     data_dir = "data"
#     create_directory(data_dir)
#     download_data(url="davidafolayan/e-commerce-dataset", dest_path=data_dir)