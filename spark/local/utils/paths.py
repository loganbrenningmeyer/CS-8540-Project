def join_path(base, *parts):
    """
    Join HDFS/Lakehouse-style paths without using pathlib.

    pathlib can mangle URI paths such as hdfs:///outputs, so Fabric scripts use
    this small string helper instead.
    """
    return "/".join([base.rstrip("/"), *[part.strip("/") for part in parts]])
