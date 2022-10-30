import tarfile


def _extract_file_content(open_tarfile, filename):
    member = open_tarfile.getmember(filename)
    file = open_tarfile.extractfile(member)
    content = file.read()
    return content


def read(archive_uri, filename=None):
    if filename is None:
        archive_uri, filename = archive_uri.rsplit(":", 1)
    with tarfile.open(archive_uri, "r") as open_tarfile:
        if isinstance(filename, str):
            file_content = _extract_file_content(open_tarfile, filename)
            return file_content
        elif isinstance(filename, list):
            for fn in filename:
                file_content = _extract_file_content(open_tarfile, fn)
                yield file_content
