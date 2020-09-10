#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import re
import tarfile
import traceback
import urllib.request
from shutil import rmtree
# NOTE that we shouldn't import pyspark here because this is used in
# setup.py, and assume there's no PySpark imported.

DEFAULT_HADOOP = "hadoop3.2"
DEFAULT_HIVE = "hive2.3"
SUPPORTED_HADOOP_VERSIONS = ["hadoop2.7", "hadoop3.2", "without-hadoop"]
SUPPORTED_HIVE_VERSIONS = ["hive1.2", "hive2.3"]
UNSUPPORTED_COMBINATIONS = [
    ("without-hadoop", "hive1.2"),
    ("hadoop3.2", "hive1.2"),
]


def checked_package_name(spark_version, hadoop_version, hive_version):
    if hive_version == "hive1.2":
        return "%s-bin-%s-%s" % (spark_version, hadoop_version, hive_version)
    else:
        return "%s-bin-%s" % (spark_version, hadoop_version)


def checked_versions(spark_version, hadoop_version, hive_version):
    """
    Check the valid combinations of supported versions in Spark distributions.

    :param spark_version: Spark version. It should be X.X.X such as '3.0.0' or spark-3.0.0.
    :param hadoop_version: Hadoop version. It should be X.X such as '2.7' or 'hadoop2.7'.
        'without' and 'without-hadoop' are supported as special keywords for Hadoop free
        distribution.
    :param hive_version: Hive version. It should be X.X such as '1.2' or 'hive1.2'.

    :return it returns fully-qualified versions of Spark, Hadoop and Hive in a tuple.
        For example, spark-3.0.0, hadoop3.2 and hive2.3.
    """
    if re.match("^[0-9]+\\.[0-9]+\\.[0-9]+$", spark_version):
        spark_version = "spark-%s" % spark_version
    if not spark_version.startswith("spark-"):
        raise RuntimeError(
            "Spark version should start with 'spark-' prefix; however, "
            "got %s" % spark_version)

    if hadoop_version == "without":
        hadoop_version = "without-hadoop"
    elif re.match("^[0-9]+\\.[0-9]+$", hadoop_version):
        hadoop_version = "hadoop%s" % hadoop_version

    if hadoop_version not in SUPPORTED_HADOOP_VERSIONS:
        raise RuntimeError(
            "Spark distribution of %s is not supported. Hadoop version should be "
            "one of [%s]" % (hadoop_version, ", ".join(
                SUPPORTED_HADOOP_VERSIONS)))

    if re.match("^[0-9]+\\.[0-9]+$", hive_version):
        hive_version = "hive%s" % hive_version

    if hive_version not in SUPPORTED_HIVE_VERSIONS:
        raise RuntimeError(
            "Spark distribution of %s is not supported. Hive version should be "
            "one of [%s]" % (hive_version, ", ".join(
                SUPPORTED_HADOOP_VERSIONS)))

    if (hadoop_version, hive_version) in UNSUPPORTED_COMBINATIONS:
        raise RuntimeError("Hive 1.2 should only be with Hadoop 2.7.")

    return spark_version, hadoop_version, hive_version


def install_spark(dest, spark_version, hadoop_version, hive_version):
    """
    Installs Spark that corresponds to the given Hadoop version in the current
    library directory.

    :param dest: The location to download and install the Spark.
    :param spark_version: Spark version. It should be spark-X.X.X form.
    :param hadoop_version: Hadoop version. It should be hadoopX.X
        such as 'hadoop2.7' or 'without-hadoop'.
    :param hive_version: Hive version. It should be hiveX.X such as 'hive1.2'.
    """

    package_name = checked_package_name(spark_version, hadoop_version, hive_version)
    package_local_path = os.path.join(dest, "%s.tgz" % package_name)
    sites = get_preferred_mirrors()
    print("Trying to download Spark %s from [%s]" % (spark_version, ", ".join(sites)))

    pretty_pkg_name = "%s for Hadoop %s" % (
        spark_version,
        "Free build" if hadoop_version == "without" else hadoop_version)

    for site in sites:
        os.makedirs(dest, exist_ok=True)
        url = "%s/spark/%s/%s.tgz" % (site, spark_version, package_name)

        tar = None
        try:
            print("Downloading %s from:\n- %s" % (pretty_pkg_name, url))
            download_to_file(urllib.request.urlopen(url), package_local_path)

            print("Installing to %s" % dest)
            tar = tarfile.open(package_local_path, "r:gz")
            for member in tar.getmembers():
                if member.name == package_name:
                    # Skip the root directory.
                    continue
                member.name = os.path.relpath(member.name, package_name + os.path.sep)
                tar.extract(member, dest)
            return
        except Exception:
            print("Failed to download %s from %s:" % (pretty_pkg_name, url))
            traceback.print_exc()
            rmtree(dest, ignore_errors=True)
        finally:
            if tar is not None:
                tar.close()
            if os.path.exists(package_local_path):
                os.remove(package_local_path)
    raise IOError("Unable to download %s." % pretty_pkg_name)


def get_preferred_mirrors():
    mirror_urls = []
    for _ in range(3):
        try:
            response = urllib.request.urlopen(
                "https://www.apache.org/dyn/closer.lua?preferred=true")
            mirror_urls.append(response.read().decode('utf-8'))
        except Exception:
            # If we can't get a mirror URL, skip it. No retry.
            pass

    default_sites = [
        "https://archive.apache.org/dist", "https://dist.apache.org/repos/dist/release"]
    return list(set(mirror_urls)) + default_sites


def download_to_file(response, path, chunk_size=1024 * 1024):
    total_size = int(response.info().get('Content-Length').strip())
    bytes_so_far = 0

    with open(path, mode="wb") as dest:
        while True:
            chunk = response.read(chunk_size)
            bytes_so_far += len(chunk)
            if not chunk:
                break
            dest.write(chunk)
            print("Downloaded %d of %d bytes (%0.2f%%)" % (
                bytes_so_far,
                total_size,
                round(float(bytes_so_far) / total_size * 100, 2)))
