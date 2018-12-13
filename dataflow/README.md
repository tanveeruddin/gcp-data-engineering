GCP Dataflow walthroughs or experiments

As a spark programmers, I found the following things missing:
* while reading csv, cant skip header file automatically. You need to purge it. As per https://beam.apache.org/releases/pydoc/2.0.0/apache_beam.io.html, it now support skp header option. :-)
* No schema auto-detect feature
