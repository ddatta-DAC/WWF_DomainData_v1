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
    logger.error('Error in preprocessing : See hs_code_cleanup_v1.HSCode_preprocessing_v3')

try:
    from src.WWF_HighRisk import  WWFHighRisk
    WWFHighRisk.main()
    logger.info('Success 2 : WWFHighRisk')
except:
    logger.error('Error in preprocessing : See WWFHighRisk.WWFHighRisk')

try:
    from src.CITES import process_CITES_data
    process_CITES_data.main()
    logger.info('Success 3 : CITES data cleaned')
except:
    logger.error('Error in preprocessing : See CITES.process_CITES_data')

try:
    from src.IUCN_Redlist import process_IUCN_Redlist
    process_IUCN_Redlist.main()
    logger.info('Success 4 : IUCN Redlist data cleaned')
except:
    logger.error('Error in preprocessing : See IUCN_Redlist.process_IUCN_Redlist')

try:
    from src.CommerciallyTraded import raw_data_parser
    raw_data_parser.main()
    logger.info('Success 5 : CommerciallyTraded list data cleaned')
except:
    logger.error('Error in preprocessing : See CommerciallyTraded.raw_data_parser')

try:
    from src.ForestProductKeywords import raw_data_parser
    raw_data_parser.main()
    logger.info('Success 6 : ForestProductKeywords list data cleaned')
except:
    logger.error('Error in preprocessing : See ForestProductKeywords.raw_data_parser')

try:
    from src.Collation import collate_plant_data_1
    collate_plant_data_1.main()
    logger.info('Success 7 : Collation done : See output :: GeneratedData/Collated')
except:
    logger.error('Error in preprocessing : See Collation.collate_plant_data_1')


# ------- end ------- #

logger.info('End : ' + str(datetime.datetime.now()))
handler.close()
logger.handlers.remove(handler)