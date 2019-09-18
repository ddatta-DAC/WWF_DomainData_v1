import os
import inspect
import sys
import logging
import logging.handlers
import datetime
# ------------------- #

OP_DIR = './../Logs'
if not os.path.exists(OP_DIR):
    os.mkdir(OP_DIR)

log_file = 'log_file.txt'
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(os.path.join(OP_DIR, log_file))
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.info('Start : ' + str(datetime.datetime.now()))
# ------------------- #
# Place modules to be executed
# ------------------- #
try:
    from src.hs_code_cleanup_v1 import  HSCode_preprocessing_v3
    HSCode_preprocessing_v3.main()
    logger.info('Success 1 : HSCode_preprocessing_v3')
except:
    logger.error('Error in preprocessing HS codes : See HSCode_preprocessing_v3')

try:
    from src.WWF_HighRisk import  WWFHighRisk
    WWFHighRisk.main()
    logger.info('Success 2 : WWFHighRisk')
except:
    logger.error('Error in preprocessing HS codes : See WWFHighRisk')

# ------- end ------- #
logger.info('End : ' + str(datetime.datetime.now()))
handler.close()
logger.handlers.remove(handler)