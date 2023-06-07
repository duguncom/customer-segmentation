import os
from datetime import date
import pandas as pd
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import src.utils as utils
import src.config as config

#read our dataset
rfm_segmentation = pd.read_csv(os.path.join("data","rfm_segmentation.csv"))

# Encoding data with Business logic
rfm_segmentation.R_Quartile[rfm_segmentation.R_Quartile == 1] = 'Haber Yok'
rfm_segmentation.R_Quartile[rfm_segmentation.R_Quartile == 2] = 'Gönlümüzdeler'
rfm_segmentation.R_Quartile[rfm_segmentation.R_Quartile == 3] = 'İletişimdeyiz'
rfm_segmentation.R_Quartile[rfm_segmentation.R_Quartile == 4] = 'Tazecik'
rfm_segmentation.F_Quartile[rfm_segmentation.F_Quartile == 1] = 'Nadiren'
rfm_segmentation.F_Quartile[rfm_segmentation.F_Quartile == 2] = 'Arada Sırada'
rfm_segmentation.F_Quartile[rfm_segmentation.F_Quartile == 3] = 'Mükerrer'
rfm_segmentation.F_Quartile[rfm_segmentation.F_Quartile == 4] = 'Sürekli'
rfm_segmentation.M_Quartile[rfm_segmentation.M_Quartile == 1] = 'Bronze'
rfm_segmentation.M_Quartile[rfm_segmentation.M_Quartile == 2] = 'Silver'
rfm_segmentation.M_Quartile[rfm_segmentation.M_Quartile == 3] = 'Gold'
rfm_segmentation.M_Quartile[rfm_segmentation.M_Quartile == 4] = 'Platinum'

Seg_score = rfm_segmentation[['provider_id','customer_id','RFMScore','RFMScore_num','satisfaction_status','payment_status_id']].copy()
Seg_score["Customer_class"] = rfm_segmentation['R_Quartile'].astype(str) +"-"+ rfm_segmentation["F_Quartile"]+"-"+ rfm_segmentation["M_Quartile"]

Seg_score['RFMScore_numb'] = Seg_score.loc[:, 'RFMScore_num']
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 3] = 'Umut yok'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 4] = 'Umut yok'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 5] = 'Potansiyeli var'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 6] = 'Potansiyeli var'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 7] = 'Potansiyeli var'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 8] = 'Geleceği parlak'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 9] = 'Geleceği parlak'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 10] = 'Geleceği parlak'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 11] = 'Elit'
Seg_score.RFMScore_num[Seg_score.RFMScore_num == 12] = 'Elit'

Seg_score.rename(columns = {'RFMScore_num':'RFMScore_cat'}, inplace = True)

# Assuming your dataframe is named 'df' and the column is named 'column_name'
Seg_score[['Recency', 'Frequency', 'Monetary']] = Seg_score['Customer_class'].str.split('-', expand=True)

# Optionally, you can remove the original column
Seg_score.drop('Customer_class', axis=1, inplace=True)

Seg_score['date'] = date.today()

Seg_score.to_csv(os.path.join("data","segmentation.csv"),index=False)

# Saving result into the DB
if utils.table_exists("staging","DUGUN_DS","B2B_segmentation") == True:
    print("Table exist, adding data to table")
    utils.write_to_db(os.path.join("data","segmentation.csv"),"staging","DUGUN_DS","B2B_segmentation")
elif utils.table_exists("staging","DUGUN_DS","B2B_segmentation") == False:
    print("Table os not exist, Creating table and adding data to table")
    #create_table("staging","DUGUN_DS")
    utils.write_to_db(os.path.join("data","segmentation.csv"),"staging","DUGUN_DS","B2B_segmentation")