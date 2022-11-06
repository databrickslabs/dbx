from urllib.parse import urlparse


def strip_databricks_url(url: str) -> str:
    """
    Mlflow API requires url to be stripped, e.g.
    {scheme}://{netloc}/some-stuff/ shall be transformed to {scheme}://{netloc}
    :param url: url to be stripped
    :return: stripped url
    """
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"
