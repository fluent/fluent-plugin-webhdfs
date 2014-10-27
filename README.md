# fluent-plugin-webhdfs

[Fluentd](http://fluentd.org/) output plugin to write data into Hadoop HDFS over WebHDFS/HttpFs.

WebHDFSOutput slices data by time (specified unit), and store these data as hdfs file of plain text. You can specify to:

* format whole data as serialized JSON, single attribute or separated multi attributes
  * or LTSV, labeled-TSV (see http://ltsv.org/ )
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

If you want JSON object only (without time or tag or both on header of lines), specify it by `output_include_time` or `output_include_tag` (default true):

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      output_include_time false
      output_include_tag  false
    </match>

To specify namenode, `namenode` is also available:

    <match access.**>
      type     webhdfs
      namenode master.your.cluster.local:50070
      path     /path/on/hdfs/access.log.%Y%m%d_%H.log
    </match>

To store data as LTSV without time and tag over WebHDFS:

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      output_data_type ltsv
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

With ssl:

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      ssl true
      ssl_ca_file /path/to/ca_file.pem   # if needed
      ssl_verify_mode peer               # if needed (peer or none)
    </match>

With kerberos authentication:

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /path/on/hdfs/access.log.%Y%m%d_%H.log
      kerberos true
    </match>

### Namenode HA / Auto retry for WebHDFS known errors

`fluent-plugin-webhdfs` (v0.2.0 or later) accepts 2 namenodes for Namenode HA (active/standby). Use `standby_namenode` like this:

    <match access.**>
      type             webhdfs
      namenode         master1.your.cluster.local:50070
	  standby_namenode master2.your.cluster.local:50070
      path             /path/on/hdfs/access.log.%Y%m%d_%H.log
    </match>

And you can also specify to retry known hdfs errors (such like `LeaseExpiredException`) automatically. With this configuration, fluentd doesn't write logs for this errors if retry successed.

    <match access.**>
      type               webhdfs
      namenode           master1.your.cluster.local:50070
      path               /path/on/hdfs/access.log.%Y%m%d_%H.log
	  retry_known_errors yes
	  retry_times        1 # default 1
	  retry_interval     1 # [sec] default 1
    </match>

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

For high load cluster nodes, you can specify timeouts for HTTP requests.

    <match access.**>
	  type webhdfs
	  namenode master.your.cluster.local:50070
      path /log/access/%Y%m%d/${hostname}.log
	  open_timeout 180 # [sec] default: 30
	  read_timeout 180 # [sec] default: 60
    </match>

### For unstable Namenodes

With default configuration, fluent-plugin-webhdfs checks HDFS filesystem status and raise error for inacive NameNodes.

If you were usging unstable NameNodes and have wanted to ignore NameNode errors on startup of fluentd, enable `ignore_start_check_error` option like below:

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      path /log/access/%Y%m%d/${hostname}.log
      ignore_start_check_error true
    </match>

### For unstable Datanodes

With unstable datanodes that frequently downs, appending over WebHDFS may produce broken files. In such cases, specify `append no` and `${chunk_id}` parameter.

    <match access.**>
      type webhdfs
      host namenode.your.cluster.local
      port 50070
      
      append no
      path   /log/access/%Y%m%d/${hostname}.${chunk_id}.log
    </match>

`out_webhdfs` creates new files on hdfs per flush of fluentd, with chunk id. You shouldn't care broken files from append operations.

## TODO

* configuration example for Hadoop Namenode HA
  * here, or docs.fluentd.org ?
* patches welcome!

## Copyright

* Copyright (c) 2012- TAGOMORI Satoshi (tagomoris)
* License
  * Apache License, Version 2.0
