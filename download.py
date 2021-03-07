import os
import requests


def download(url: str):
    filename = "owid-covid-data.csv"  # be careful with file names
    file_path = os.path.join(filename)

    r = requests.get(url, stream=True)
    if r.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))


download("https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv")
