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

      field_separator TAB        # or 'SPACE', 'COMMA'
      output_include_time false
      output_include_tag true
      remove_prefix access

      output_data_type attr:path,status,referer,agent,bytes
    </match>

If message doesn't have specified attribute, fluent-plugin-webhdfs outputs 'NULL' instead of values.

## TODO

* long run test
  * over webhdfs and httpfs
* patches welcome!

## Copyright

* Copyright (c) 2012- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, Version 2.0
