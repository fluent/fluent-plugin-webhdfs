# fluent-plugin-webhdfs

[![Build Status](https://travis-ci.org/fluent/fluent-plugin-webhdfs.svg?branch=master)](https://travis-ci.org/fluent/fluent-plugin-webhdfs)

[Fluentd](http://fluentd.org/) output plugin to write data into Hadoop HDFS over WebHDFS/HttpFs.

"webhdfs" output plugin formats data into plain text, and store it as files on HDFS. This plugin supports:

* inject tag and time into record (and output plain text data) using `<inject>` section
* format events into plain text by format plugins using `<format>` section
* control flushing using `<buffer>` section

Paths on HDFS can be generated from event timestamp, tag or any other fields in records.

## Requirements

| fluent-plugin-webhdfs | fluentd    | ruby   |
|-----------------------|------------|--------|
| >= 1.0.0              | >= v0.14.4 | >= 2.1 |
| <  1.0.0              | <  v0.14.0 | >= 1.9 |

### Older versions

The versions of `0.x.x` of this plugin are for older version of Fluentd (v0.12.x). Old style configuration parameters (using `output_data_type`, `output_include_*` or others) are still supported, but are deprecated.
Users should use `<format>` section to control how to format events into plain text.

## Configuration

### WebHDFSOutput

To store data by time,tag,json (same with '@type file') over WebHDFS:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
    </match>

If you want JSON object only (without time or tag or both on header of lines), use `<format>` section to specify `json` formatter:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      <format>
        @type json
      </format>
    </match>

To specify namenode, `namenode` is also available:

    <match access.**>
      @type     webhdfs
      namenode master.your.cluster.local:50070
      path     /path/on/hdfs/access.log.%Y%m%d_%H.log
    </match>

To store data as JSON, including time and tag (using `<inject>`), over WebHDFS:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      <buffer>
        timekey_zone -0700 # to specify timezone used for "path" time placeholder formatting
      </buffer>
      <inject>
        tag_key   tag
        time_key  time
        time_type string
        timezone  -0700
      </inject>
      <format>
        @type json
      </format>
    </match>

To store data as JSON, including time as unix time, using path including tag as directory:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/${tag}/access.log.%Y%m%d_%H.log
      <buffer time,tag>
        @type   file                    # using file buffer
        path    /var/log/fluentd/buffer # buffer directory path
        timekey 3h           # create a file per 3h
        timekey_use_utc true # time in path are formatted in UTC (default false means localtime)
      </buffer>
      <inject>
        time_key  time
        time_type unixtime
      </inject>
      <format>
        @type json
      </format>
    </match>

With username of pseudo authentication:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      username hdfsuser
    </match>
      
Store data over HttpFs (instead of WebHDFS):

    <match access.**>
      @type webhdfs
      host httpfs.node.your.cluster.local
      port 14000
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      httpfs true
    </match>

With ssl:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      ssl true
      ssl_ca_file /path/to/ca_file.pem   # if needed
      ssl_verify_mode peer               # if needed (peer or none)
    </match>

Here `ssl_verify_mode peer` means to verify the server's certificate.
You can turn off it by setting `ssl_verify_mode none`. The default is `peer`.
See [net/http](http://www.ruby-doc.org/stdlib-2.1.3/libdoc/net/http/rdoc/Net/HTTP.html)
and [openssl](http://www.ruby-doc.org/stdlib-2.1.3/libdoc/openssl/rdoc/OpenSSL.html) documentation for further details.

With kerberos authentication:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      kerberos true
      kerberos_keytab /path/to/keytab # if needed
      renew_kerberos_delegation_token true # if needed
    </match>

NOTE: You need to install `gssapi` gem for kerberos. See https://github.com/kzk/webhdfs#for-kerberos-authentication

If you want to compress data before storing it:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H
      compress gzip  # or 'bzip2', 'snappy', 'hadoop_snappy', 'lzo_command', 'zstd'
    </match>

Note that if you set `compress gzip`, then the suffix `.gz` will be added to path (or `.bz2`, `.sz`, `.snappy`, `.lzo`, `.zst`).
Note that you have to install additional gem for several compress algorithms:

- snappy: install snappy gem
- hadoop_snappy: install snappy gem
- bzip2: install bzip2-ffi gem
- zstd: install zstandard gem

Note that zstd will require installation of the libzstd native library. See the [zstandard-ruby](https://github.com/msievers/zstandard-ruby#examples-for-installing-libzstd) repo for infomration on the required packages for your operating system.

You can also specify compression block size (currently supported only for Snappy codecs):

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H
      compress hadoop_snappy
      block_size 32768
    </match>

If you want to explicitly specify file extensions in HDFS (override default compressor extensions):

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H
      compress snappy
      extension ".snappy"
    </match>

With this configuration paths in HDFS will be like `/path/on/hdfs/access.log.20201003_12.snappy`.
This one may be useful when (for example) you need to use snappy codec but `.sz` files are not recognized as snappy files in HDFS.

### Namenode HA / Auto retry for WebHDFS known errors

`fluent-plugin-webhdfs` (v0.2.0 or later) accepts 2 namenodes for Namenode HA (active/standby). Use `standby_namenode` like this:

    <match access.**>
      @type            webhdfs
      namenode         master1.your.cluster.local:50070
	  standby_namenode master2.your.cluster.local:50070
      path             /path/on/hdfs/access.log.%Y%m%d_%H.log
    </match>

And you can also specify to retry known hdfs errors (such like `LeaseExpiredException`) automatically. With this configuration, fluentd doesn't write logs for this errors if retry successed.

    <match access.**>
      @type              webhdfs
      namenode           master1.your.cluster.local:50070
      path               /path/on/hdfs/access.log.%Y%m%d_%H.log
	  retry_known_errors yes
	  retry_times        1 # default 1
	  retry_interval     1 # [sec] default 1
    </match>

### Performance notifications

Writing data on HDFS single file from 2 or more fluentd nodes, makes many bad blocks of HDFS. If you want to run 2 or more fluentd nodes with fluent-plugin-webhdfs, you should configure 'path' for each node.
To include hostname, `#{Socket.gethostname}` is available in Fluentd configuration string literals by ruby expression (in `"..."` strings). This plugin also supports `${uuid}` placeholder to include random uuid in paths.

For hostname:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path "/log/access/%Y%m%d/#{Socket.gethostname}.log" # double quotes needed to expand ruby expression in string
    </match>

Or with random filename (to avoid duplicated file name only):

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /log/access/%Y%m%d/${uuid}.log
    </match>

With configurations above, you can handle all of files of `/log/access/20120820/*` as specified timeslice access logs.

For high load cluster nodes, you can specify timeouts for HTTP requests.

    <match access.**>
	  @type webhdfs
	  namenode master.your.cluster.local:50070
      path /log/access/%Y%m%d/${hostname}.log
	  open_timeout 180 # [sec] default: 30
	  read_timeout 180 # [sec] default: 60
    </match>

### For unstable Namenodes

With default configuration, fluent-plugin-webhdfs checks HDFS filesystem status and raise error for inactive NameNodes.

If you were using unstable NameNodes and have wanted to ignore NameNode errors on startup of fluentd, enable `ignore_start_check_error` option like below:

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /log/access/%Y%m%d/${hostname}.log
      ignore_start_check_error true
    </match>

### For unstable Datanodes

With unstable datanodes that frequently downs, appending over WebHDFS may produce broken files. In such cases, specify `append no` and `${chunk_id}` parameter.

    <match access.**>
      @type webhdfs
      host namenode.your.cluster.local
      port 50070
      
      append no
      path   "/log/access/%Y%m%d/#{Socket.gethostname}.${chunk_id}.log"
    </match>

`out_webhdfs` creates new files on hdfs per flush of fluentd, with chunk id. You shouldn't care broken files from append operations.

## TODO

* patches welcome!

## Copyright

* Copyright (c) 2012- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, Version 2.0
