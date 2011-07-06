#!/usr/bin/env python
import os, sys
sys.path.append("..")

from runtests import django_tests 

# sqlite tests
os.environ['PYTHONPATH'] = "~/Desktop/django_hack/gsoc2011/django"
os.environ['DJANGO_SETTINGS_MODULE'] = "test_sqlite"
failures = django_tests(1, True, True, ["schema_alteration"])
if failures:
    sys.exit(bool(failures))