# fluent-plugin-webhdfs

Fluentd output plugin to write data into Hadoop HDFS over WebHDFS/HttpFs.

WebHDFSOutput slices data by time (specified unit), and store these data as hdfs file of plain text. You can specify to:

* format whole data as serialized JSON, single attribute or separated multi attributes
* include time as line header, or not
* include tag as line header, or not
* change field separator (default: TAB)
* add new line as termination, or not

And you can specify output file path as 'path /path/to/dir/access.%Y%m%d.log', then got '/path/to/dir/access.20120316.log' on HDFS.

## Configuration

### WebHDFSOutput

To store data by time,tag,json (same with 'type file') over WebHDFS:

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
    </match>

With username of pseudo authentication:

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      username hdfsuser
    </match>
      
Store data over HttpFs (instead of WebHDFS):

    <match access.**>
      type webhdfs
      host httpfs.node.your.cluster.local
      port 14000
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      httpfs true
    </match>

Store data as TSV (TAB separated values) of specified keys, without time, with tag (removed prefix 'access'):

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log

      field_separator TAB        # or 'SPACE', 'COMMA' or 'SOH'(Start Of Heading: \001)
      output_include_time false
      output_include_tag true
      remove_prefix access

      output_data_type attr:path,status,referer,agent,bytes
    </match>

If message doesn't have specified attribute, fluent-plugin-webhdfs outputs 'NULL' instead of values.

### Performance notifications

Writing data on HDFS single file from 2 or more fluentd nodes, makes many bad blocks of HDFS. If you want to run 2 or more fluentd nodes with fluent-plugin-webhdfs, you should configure 'path' for each node.
You can use '${hostname}' or '${uuid:random}' placeholders in configuration for this purpose.

For hostname:

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /log/access/%Y%m%d/${hostname}.log
    </match>

Or with random filename (to avoid duplicated file name only):

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /log/access/%Y%m%d/${uuid:random}.log
    </match>

With configurations above, you can handle all of files of '/log/access/20120820/*' as specified timeslice access logs.

## TODO

* patches welcome!

## Copyright

* Copyright (c) 2012- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, Version 2.0
