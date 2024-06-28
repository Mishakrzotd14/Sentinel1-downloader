import os
import shutil
import sys

import boto3
import geopandas as gpd
import requests
from fp.fp import FreeProxy
from shapely.geometry import box
from tqdm import tqdm

ACCESS_KEY = "5ULUPKF3CZOGCM2D7ZKA"
SECRET_KEY = "Gs92vgFmj9fVjxSO6riusaBYeekNyCmfBvD4YNUF"

CRS_FOR_COPERNICUS = "epsg:4326"
PLATFORM = "S1A_IW_GRD"

SRID = "4326"
TIME_FORMAT = "T00:00:00.000Z"
TIME_END_FORMAT = "T12:00:00.000Z"
ENDPOINT_URL = "https://eodata.dataspace.copernicus.eu/"
BUCKET = "eodata"


def get_folder_size(folder_path: str) -> int:
    """Получить размер папки."""
    total_size = 0
    for path, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(path, file)
            try:
                total_size += os.path.getsize(file_path)
            except OSError as e:
                print(f"Could not calculate the size of {file_path}: {e}")
    return total_size


def make_path(path: str) -> None:
    """Создать путь."""
    try:
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        pass
    except OSError as e:
        print(f"Error creating directory {path}: {e}")


def generate_filter_query(qp) -> str:
    """Сгенерировать строку фильтра для запроса к хранилищу данных."""
    filter_query = (
        f"contains(Name, '{qp['setillite']}') "
        f"and OData.CSC.Intersects(area=geography'SRID={SRID};{qp['footprint']}') "
        f"and ContentDate/Start gt {qp['date_start']}{TIME_FORMAT} "
        f"and ContentDate/Start lt {qp['date_end']}{TIME_END_FORMAT}"
    )
    return filter_query


def get_s3path(qp):
    """Получить путь к Amazon S3 bucket odata, используя метаданные запроса dataspace.copernicus.eu."""
    try:
        filter_query = generate_filter_query(qp)
        all_proxies = FreeProxy(timeout=1, rand=True).get_proxy_list(repeat=False)

        for proxy in all_proxies:
            proxies = {"http": proxy, "https": proxy}
            url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter={filter_query}"

            try:
                result = requests.get(url, timeout=60, allow_redirects=False, proxies=proxies).json()
                if result.get("value"):
                    products_s3path = [
                        product["S3Path"] for product in result["value"] if not product["Name"].endswith("CARD_BS")
                    ]
                    if products_s3path:
                        return products_s3path
                    else:
                        print("No products found in CDSE catalogue")
                        sys.exit()
                else:
                    print("No tiles found in CDSE catalogue with the stated parameters")
                    sys.exit()

            except requests.RequestException as e:
                print(f"Error with proxy {proxy}")
                continue
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit()


def download_file(resource, obj, target_directory: str):
    """Загрузить файлы снимков."""
    target = os.path.join(target_directory, obj.key)

    if obj.key.endswith("/"):
        make_path(target)
    else:
        dirname = os.path.dirname(target)
        if dirname != "":
            make_path(dirname)
        resource.Object(BUCKET, obj.key).download_file(target)


def download_sentinel_images(access_key: str, secret_key: str, qp, target_directory: str):
    """Загрузить изображения спутника Sentinel-2."""
    try:
        s3_path = get_s3path(qp)
        products_path = []
        for s3path_prod in s3_path:
            s3path = s3path_prod.removeprefix(f"/{BUCKET}/")
            file_name = s3path.split("/")[-1]
            dirname = os.path.dirname(s3path)

            if not os.path.exists(os.path.join(target_directory, dirname)):
                make_path(os.path.join(target_directory, dirname))
            fls = os.listdir(os.path.join(target_directory, dirname))

            resource = boto3.resource(
                service_name="s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=ENDPOINT_URL,
            )
            s3path = s3path + ""
            objects = list(resource.Bucket(BUCKET).objects.filter(Prefix=s3path))

            s3_size = sum([obj.size for obj in objects])
            if not objects:
                raise Exception(f"could not find product '' in CDSE object store")

            size_directory = get_folder_size(os.path.join(target_directory, s3path))

            download_location = os.path.join(target_directory, s3path)

            if file_name in fls and s3_size == size_directory:
                print(f"Файл {file_name} находится в папке")
                products_path.append(download_location)
            else:
                if os.path.exists(os.path.join(download_location)):
                    shutil.rmtree(os.path.join(download_location))

                print(f"\nФайл {file_name} не находится в папке. Скачивание...")

                for obj in tqdm(objects, desc=f"Downloading {file_name}", unit="file"):
                    download_file(resource, obj, target_directory)
                print(f"Снимок {file_name} скачан!")
                products_path.append(download_location)

        return products_path
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit()


if __name__ == "__main__":
    dir_download = r"D:/image"

    time_start = input("Введите начальную дату съёмки yyyy-mm-dd\n")
    time_end = input("Введите конечную  дату съёмки yyyy-mm-dd\n")

    shp_file = gpd.read_file("V:/Granica_BLR/GRRB.shp")

    if shp_file.crs != CRS_FOR_COPERNICUS:
        shp_file.to_crs(CRS_FOR_COPERNICUS, inplace=True)

    bounds = shp_file.total_bounds
    footprint = box(round(bounds[0], 3), round(bounds[1], 3), round(bounds[2], 3), round(bounds[3], 3)).wkt

    query_params = {
        "setillite": PLATFORM,
        "footprint": footprint,
        "date_start": time_start,
        "date_end": time_end,
    }

    s1_image_paths = download_sentinel_images(ACCESS_KEY, SECRET_KEY, query_params, dir_download)
