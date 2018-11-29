import os
import re
import findspark
import pandas as pd
import urllib.request

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

findspark.init()
print('ca marche bro')

# import matplotlib
# import matplotlib.pyplot as plt
# %matplotlib inline

# import imports
# from spark_set import set_spark


# def run():
#     findspark.init()
#     print('ca chemar bro ')
#
# if __name__ == '__main__':
#     run()
