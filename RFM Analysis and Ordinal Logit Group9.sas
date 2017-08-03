/*code to merge all the files*/

libname saltsnck "F:\yxv150430\LTWD\";
options user=saltsnck;
run;

proc import datafile="F:\sxv152830\saltsnck\prod_saltsnck" out=saltsnck.ProductDetails dbms=xls replace;
getnames=yes;
run;

data saltsnck.DrPanel;
infile 'F:\sxv152830\saltsnck\saltsnck_PANEL_DR_1114_1165.dat' firstobs=2 MISSOVER delimiter='09'x;
input PANID WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
run;

data saltsnck.GrPanel;
infile 'F:\sxv152830\saltsnck\saltsnck_PANEL_GR_1114_1165.dat' firstobs=2 MISSOVER delimiter='09'x;
input PANID WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
run;

data saltsnck.MaPanel;
infile 'F:\sxv152830\saltsnck\saltsnck_PANEL_MA_1114_1165.dat' firstobs=2 MISSOVER delimiter='09'x;
input PANID WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
run;

data saltsnck.panel;
set saltsnck.DrPanel saltsnck.GrPanel saltsnck.MaPanel;
UPC_Code = put(strip(COLUPC), 14.);
if lengthn(strip(UPC_Code))= 14 then UPC_Code=UPC_Code;
    else UPC_Code=repeat('0',14-lengthn(strip(UPC_Code))-1)||UPC_Code;
run;

proc print data=saltsnck.panel (obs=10);run;
proc contents data=saltsnck.panel (obs=10);run;

proc import datafile="F:\sxv152830\saltsnck\IRI week translation1" out=saltsnck.WeekData dbms=xls replace;
getnames=yes;
run;

data saltsnck.WeekData(drop=D E F G);
set saltsnck.WeekData;
run;

proc contents data=saltsnck.store (obs=10);run;

proc import datafile="F:\sxv152830\saltsnck\ads demo1.csv" out=saltsnck.demo dbms=csv replace;
getnames=yes;
run;

/*COUNTY	HH_AGE	HH_EDU	HH_OCC MALE_SMOKE FEM_SMOKE HISP_FLAG	HISP_CAT	HH Head Race (RACE2)	HH Head Race (RACE3)	
Microwave Owned by HH	ZIPCODE	FIPSCODE	market based upon zipcode	IRI Geography Number	EXT_FACT*/
 
Data saltsnck.productdetails;
set saltsnck.productdetails;
SY1=SY+0;
GE1=GE+0;
VEND1=VEND+0;
ITEM1=ITEM+0;
run;

/* correcting the UPC code */
Data saltsnck.productdetails(drop= L1 Level SY GE VEND ITEM _STUBSPEC_1431RC);
set saltsnck.ProductDetails;
length UPC_Code $14;
SY_n=STRIP(SY1);
GE_n=STRIP(GE1);
VEND_n=STRIP(VEND1);
ITEM_n=STRIP(ITEM1);
if (length(trim(SY_n))=1) then SY_n='0'||trim(SY_n);
if (length(trim(GE_n))=1) then GE_n='0'||trim(GE_n);
if lengthn(ITEM_n)= 5 then ITEM_n=ITEM_n;
    else ITEM_n=repeat('0',5-lengthn(ITEM_n)-1)||ITEM_n;
if lengthn(VEND_n)= 5 then VEND_n=VEND_n;
    else VEND_n=repeat('0',5-lengthn(VEND_n)-1)||VEND_n;
UPC_Code=trim(SY_n)|| trim(GE_n) || trim(VEND_n) || trim(ITEM_n);
run;

proc print data=saltsnck.productdetails (obs=10);run;
proc sort data=saltsnck.productdetails;
by UPC_Code;
run;

/* merge between product and panel */

proc sql;
create table saltsnck.panel_prod as select * from saltsnck.panel as store,saltsnck.productdetails as Prod where Prod.UPC_Code = store.UPC_Code;

proc print data=saltsnck.panel_prod(obs=10);
run;

proc sql;
create table saltsnck.saltproducts_pringles as select * from saltsnck.panel_prod 
where (L5 Like '%PRINGLES%');
 
PROC SQL ;
create table saltsnck.rfm_pringles_data as select PANID, MAX(WEEK) as R, SUM(UNITS) as F, SUM(DOLLARS) as M 
from saltsnck.saltproducts_pringles
group by PANID;

/* Lays RFM */
proc sql;
create table saltsnck.saltproducts_lay as select * from saltsnck.panel_prod 
where (L5 Like '%LAY%');

PROC SQL ;
create table saltsnck.rfm_lays_data as select PANID, MAX(WEEK) as R, SUM(UNITS) as F, SUM(DOLLARS) as M 
from saltsnck.saltproducts_lay
group by PANID;
/* Doritos */
proc sql;
create table saltsnck.saltproducts_doritos as select * from saltsnck.panel_prod 
where (L5 Like '%DORITOS%');

PROC SQL ;
create table saltsnck.rfm_doritos_data as select PANID, MAX(WEEK) as R, SUM(UNITS) as F, SUM(DOLLARS) as M 
from saltsnck.saltproducts_doritos
group by PANID;

/* Wise RFM*/
proc sql;
create table saltsnck.saltproducts_wise as select * from saltsnck.panel_prod 
where (L5 Like '%WISE%');

PROC SQL ;
create table saltsnck.rfm_wise_data as select PANID, MAX(WEEK) as R, SUM(UNITS) as F, SUM(DOLLARS) as M 
from saltsnck.saltproducts_wise
group by PANID;

/* Tositos*/
proc sql;
create table saltsnck.saltproducts_tostitos as select * from saltsnck.panel_prod 
where (L5 Like '%TOSTITOS%');

PROC SQL ;
create table saltsnck.rfm_tostitos_data as select PANID, MAX(WEEK) as R, SUM(UNITS) as F, SUM(DOLLARS) as M 
from saltsnck.saltproducts_tostitos
group by PANID;


/* proc rank for PRINGLES*/

proc rank data=saltsnck.rfm_pringles_data out=saltsnck.pringles_rank group=5 ties=low;
	var R F M;
	ranks R_rank F_rank M_rank;
run;

proc print data = saltsnck.pringles_rank(obs = 10);run;


proc sql;
select max(R_rank) , max(F_rank), max(M_rank) from saltsnck.pringles_rank;

data saltsnck.pringles_rank_weight;
set saltsnck.pringles_rank;
if R_rank = 2 or R_rank = 3 or R_rank = 4 then R_rank_w = R_rank * 2;
	else R_rank_w = R_rank * 1;

if M_rank = 4 or M_rank = 3 then M_rank_w = M_rank * 3;
if M_rank = 2 then M_rank_w = M_rank * 2;
	else M_rank_w = M_rank * 1;

F_rank_w = F_rank;
score = R_rank + F_rank + M_rank;
score_w = R_rank_w + F_rank_w + M_rank_w;
brand = 'Pringles';
run;

proc rank data=saltsnck.pringles_rank_weight out=saltsnck.pringles_weighted_rank group=4 ties=low;
	var score_w;
	ranks score_w_rank;
run;

/* merging pringles panel and demo */
proc sql;
create table saltsnck.saltproducts_pringles_demo as select * from saltsnck.pringles_weighted_rank as panel, saltsnck.demo as d where panel.PANID=d.Panelist_ID;

proc means data = saltsnck.saltproducts_pringles_demo ;class score_w_rank;run;

proc export data=saltsnck.saltproducts_pringles_demo
		outfile= 'F:\yxv150430\upc_pringlespanel_demo3.csv'
		dbms=csv
		replace;
run;

data saltproducts_pringles_demo_trim (drop =  R F M R_rank F_rank M_rank R_rank_w M_rank_w F_rank_w score score_w brand Panelist_ID Panelist_Type COUNTY HH_AGE HH_EDU HH_OCC MALE_SMOKE FEM_SMOKE Language Number_of_TVs_Used_by_HH Number_of_TVs_Hooked_to_Cable Year HISP_FLAG HISP_CAT HH_Head_Race__RACE2_ HH_Head_Race__RACE3_ Microwave_Owned_by_HH ZIPCODE FIPSCODE market_based_upon_zipcode IRI_Geography_Number EXT_FACT);
set saltproducts_pringles_demo;
if Combined_Pre_Tax_Income_of_HH > 0;
if Age_Group_Applied_to_Female_HH >0;
if Type_of_Residential_Possession >0;
run;

proc logistic data =saltproducts_pringles_demo_trim DESC;
class  Combined_Pre_Tax_Income_of_HH (REF="1")
Family_Size (REF="1")
HH_RACE (REF="1")
Type_of_Residential_Possession Age_Group_Applied_to_Male_HH (REF="1") Education_Level_Reached_by_Male (REF="1")
Occupation_Code_of_Male_HH (REF="0") Male_Working_Hour_Code (REF="1") 
Age_Group_Applied_to_Female_HH (REF="1") Education_Level_Reached_by_Femal  (REF="1") 
Occupation_Code_of_Female_HH (REF="0") Female_Working_Hour_Code (REF="1")  Children_Group_Code Marital_Status (REF="1"); 
id PANID;
model score_w_rank = Combined_Pre_Tax_Income_of_HH Family_Size HH_RACE Type_of_Residential_Possession Age_Group_Applied_to_Male_HH Education_Level_Reached_by_Male Occupation_Code_of_Male_HH Male_Working_Hour_Code Age_Group_Applied_to_Female_HH Education_Level_Reached_by_Femal Occupation_Code_of_Female_HH Female_Working_Hour_Code   Children_Group_Code Marital_Status;
run;

/* proc rank for lays*/

proc rank data=saltsnck.rfm_lays_data out=saltsnck.lays_rank group=5 ties=low;
	var R F M;
	ranks R_rank F_rank M_rank;
run;

/*proc print data = saltsnck.lays_rank(obs = 10);run;*/


data saltsnck.lays_rank_weight;
set saltsnck.lays_rank;
if R_rank = 2 or R_rank = 3 or R_rank = 4 then R_rank_w = R_rank * 2;
	else R_rank_w = R_rank * 1;

if M_rank = 4 or M_rank = 3 then M_rank_w = M_rank * 3;
if M_rank = 2 then M_rank_w = M_rank * 2;
	else M_rank_w = M_rank * 1;

F_rank_w = F_rank;
score = R_rank + F_rank + M_rank;
score_w = R_rank_w + F_rank_w + M_rank_w;
brand = 'Lays';
run;


proc rank data=saltsnck.lays_rank_weight out=saltsnck.lays_weighted_rank group=4 ties=low;
	var score_w;
	ranks score_w_rank;
run;

/* merging pringles panel and demo */
proc sql;
create table saltsnck.saltproducts_lays_demo as select * from saltsnck.lays_weighted_rank as panel, saltsnck.demo as d where panel.PANID=d.Panelist_ID;

proc export data=saltsnck.saltproducts_lays_demo
		outfile= 'F:\yxv150430\upc_lays_panel_demo2.csv'
		dbms=csv
		replace;
run;

/* proc rank for doritos*/

proc rank data=saltsnck.rfm_doritos_data out=saltsnck.doritos_rank group=5 ties=low;
	var R F M;
	ranks R_rank F_rank M_rank;
run;

/*proc print data = saltsnck.lays_rank(obs = 10);run;*/


data saltsnck.doritos_rank_weight;
set saltsnck.doritos_rank;
if R_rank = 2 or R_rank = 3 or R_rank = 4 then R_rank_w = R_rank * 2;
	else R_rank_w = R_rank * 1;

if M_rank = 4 or M_rank = 3 then M_rank_w = M_rank * 3;
if M_rank = 2 then M_rank_w = M_rank * 2;
	else M_rank_w = M_rank * 1;

F_rank_w = F_rank;
score = R_rank + F_rank + M_rank;
score_w = R_rank_w + F_rank_w + M_rank_w;
brand = 'doritios';
run;


proc rank data=saltsnck.doritos_rank_weight out=saltsnck.doritos_weighted_rank group=4 ties=low;
	var score_w;
	ranks score_w_rank;
run;

/* merging pringles panel and demo */
proc sql;
create table saltsnck.saltproducts_doritos_demo as select * from saltsnck.doritos_weighted_rank as panel, saltsnck.demo as d where panel.PANID=d.Panelist_ID;

proc export data=saltsnck.saltproducts_doritos_demo
		outfile= 'F:\yxv150430\upc_doritos_panel_demo.csv'
		dbms=csv
		replace;
run;

/* proc rank for wise*/

proc rank data=saltsnck.rfm_wise_data out=saltsnck.wise_rank group=5 ties=low;
	var R F M;
	ranks R_rank F_rank M_rank;
run;

/*proc print data = saltsnck.lays_rank(obs = 10);run;*/


data saltsnck.wise_rank_weight;
set saltsnck.wise_rank;
if R_rank = 2 or R_rank = 3 or R_rank = 4 then R_rank_w = R_rank * 2;
	else R_rank_w = R_rank * 1;

if M_rank = 4 or M_rank = 3 then M_rank_w = M_rank * 3;
if M_rank = 2 then M_rank_w = M_rank * 2;
	else M_rank_w = M_rank * 1;

F_rank_w = F_rank;
score = R_rank + F_rank + M_rank;
score_w = R_rank_w + F_rank_w + M_rank_w;
brand = 'wise';
run;


proc rank data=saltsnck.wise_rank_weight out=saltsnck.wise_weighted_rank group=4 ties=low;
	var score_w;
	ranks score_w_rank;
run;

/* merging pringles panel and demo */
proc sql;
create table saltsnck.saltproducts_wise_demo as select * from saltsnck.wise_weighted_rank as panel, saltsnck.demo as d where panel.PANID=d.Panelist_ID;

proc export data=saltsnck.saltproducts_wise_demo
		outfile= 'F:\yxv150430\upc_wise_panel_demo.csv'
		dbms=csv
		replace;
run;


/* proc rank for tostitos*/

proc rank data=saltsnck.rfm_tostitos_data out=saltsnck.tostitos_rank group=5 ties=low;
	var R F M;
	ranks R_rank F_rank M_rank;
run;

/*proc print data = saltsnck.lays_rank(obs = 10);run;*/


data saltsnck.tostitos_rank_weight;
set saltsnck.tostitos_rank;
if R_rank = 2 or R_rank = 3 or R_rank = 4 then R_rank_w = R_rank * 2;
	else R_rank_w = R_rank * 1;

if M_rank = 4 or M_rank = 3 then M_rank_w = M_rank * 3;
if M_rank = 2 then M_rank_w = M_rank * 2;
	else M_rank_w = M_rank * 1;

F_rank_w = F_rank;
score = R_rank + F_rank + M_rank;
score_w = R_rank_w + F_rank_w + M_rank_w;
brand = 'tostitos';
run;


proc rank data=saltsnck.tostitos_rank_weight out=saltsnck.tostitos_weighted_rank group=4 ties=low;
	var score_w;
	ranks score_w_rank;
run;

/* merging pringles panel and demo */
proc sql;
create table saltsnck.saltproducts_tostitos_demo as select * from saltsnck.tostitos_weighted_rank as panel, saltsnck.demo as d where panel.PANID=d.Panelist_ID;


data saltsnck.all_brands_rfm;
set 
saltsnck.saltproducts_pringles_demo
saltsnck.saltproducts_lays_demo
saltsnck.saltproducts_doritos_demo
saltsnck.saltproducts_tostitos_demo
saltsnck.saltproducts_wise_demo;
run;


proc export data=saltsnck.all_brands_rfm
		outfile= 'F:\yxv150430\ltwd\all_brands_rfm.csv'
		dbms=csv
		replace;
run;

