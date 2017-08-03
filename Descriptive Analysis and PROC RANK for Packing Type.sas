libname snaks "F:\saltsnck";

/* Import drugs store dataset */
data snaks.drug_store;
infile 'F:\saltsnck\saltsnck_drug_1114_1165' firstobs=2 MISSOVER;
input IRI_KEY 1-7 WEEK 9-12 SY $ 14-15 GE $ 17-18 VEND $ 20-24 ITEM $ 26-30 UNITS 32-36 DOLLARS 38-45 F $ 47-50 D 52 PR 54;
if (length(trim(SY))=1) then SY='0'||trim(SY);
if (length(trim(GE))=1) then GE='0'||trim(GE);
if lengthn(ITEM)= 5 then ITEM=ITEM;
    else ITEM=repeat('0',5-lengthn(ITEM)-1)||ITEM;
if lengthn(VEND)= 5 then VEND=VEND;
    else VEND=repeat('0',5-lengthn(VEND)-1)||VEND;
Outlet='DR';
UPC_Code=trim(SY)|| trim(GE) || trim(VEND) || trim(ITEM);
run;

data snaks.drug_store(drop = SY GE VEND ITEM);
set snaks.drug_store;
run;

/* Import groceries store dataset */
data snaks.groc_store;
infile 'F:\saltsnck\saltsnck_groc_1114_1165' firstobs=2 MISSOVER;
input IRI_KEY 1-7 WEEK 9-12 SY $ 14-15 GE $ 17-18 VEND $ 20-24 ITEM $ 26-30 UNITS 32-36 DOLLARS 38-45 F $ 47-50 D 52 PR 54;
if (length(trim(SY))=1) then SY='0'||trim(SY);
if (length(trim(GE))=1) then GE='0'||trim(GE);
if lengthn(ITEM)= 5 then ITEM=ITEM;
    else ITEM=repeat('0',5-lengthn(ITEM)-1)||ITEM;
if lengthn(VEND)= 5 then VEND=VEND;
    else VEND=repeat('0',5-lengthn(VEND)-1)||VEND;
Outlet='Gr';
UPC_Code=trim(SY)|| trim(GE) || trim(VEND) || trim(ITEM);
run;

data snaks.groc_store(drop = SY GE VEND ITEM);
set snaks.groc_store;
run;

/* Import product dataset */
proc import datafile="F:\saltsnck\prod_saltsnck" out=snaks.prod_data dbms=xls replace;
getnames=yes;
run;

Data snaks.prod_data;
set snaks.prod_data;
SY1=SY+0;
GE1=GE+0;
VEND1=VEND+0;
ITEM1=ITEM+0;
run;


/* correcting the UPC code */
Data snaks.prod_data(drop= L1 Level SY GE VEND ITEM _STUBSPEC_1431RC);
set snaks.prod_data;
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

proc print data = snaks.prod_data (obs = 20);run;

data snaks.prod_data(drop = UPC SY_n GE_n VEND_n ITEM_n FAT_CONTENT COOKING_METHOD SALT_SODIUM_CONTENT);
set snaks.prod_data;
rename L2 = cateogry L3 = parent_comp L4 = sub_comp L5 = brand;
Oz1 = scan(L9,-1," "); 
Oz = compress(Oz1, "OZ");
Oz_num = input(Oz, 3.);
drop Oz;
rename Oz_num = Oz;
drop Oz1; 
run;

data snaks.prod_data(drop = SY1 GE1 VEND1 ITEM1 L9);
set snaks.prod_data;
run;





/* Import panel level dataset */
data snaks.panel_drug;
infile 'F:\saltsnck\saltsnck_PANEL_DR_1114_1165.txt' firstobs=2 MISSOVER delimiter='09'x;
input PANID WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
run;

data snaks.panel_groc;
infile 'F:\saltsnck\saltsnck_PANEL_GR_1114_1165.dat' firstobs=2 MISSOVER delimiter='09'x;
input PANID WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
run;

data snaks.panel_groc;
set snaks.panel_groc;
COLUPC1 = put(strip(COLUPC),14.);
if lengthn(strip(COLUPC))= 14 then COLUPC1=COLUPC;
    else COLUPC1=repeat('0',14-lengthn(strip(COLUPC))-1)||strip(COLUPC);
run;

proc contents data = snaks.panel_groc;run;

/*
data snaks.panel_ma;
infile 'F:\saltsnck\saltsnck_PANEL_MA_1114_1165.dat' firstobs=2 MISSOVER delimiter='09'x;
input PANID WEEK UNITS OUTLET$ DOLLARS IRI_KEY COLUPC;
run;
*/

/* import week level translation dataset */
proc import datafile="F:\saltsnck\IRI week translation2" out=snaks.WeekData dbms=xls replace;
getnames=yes;
run;

data snaks.WeekData(drop=D E F G);
set snaks.WeekData;
run;

/* importing delivery stores dataset */
data snaks.Del_Str;
infile 'F:\saltsnck\Delivery_Stores.dat' firstobs=2 MISSOVER;
input IRI_KEY 1-7 OU $ 9-10  EST_ACV 12-19 Market_Name $ 21-44 Open 46-49 Clsd 51-54 MskdName $ 56-63;
run;

/* create a unique delivery stores */
proc sql;
create table snaks.Del_Str2 as select IRI_KEY, Market_Name, max(EST_ACV) as EST_ACV from snaks.Del_Str group by IRI_KEY, Market_Name;

/* categorizing the stores as "small", "medium" and "large" */
data snaks.Del_Str2;
set snaks.Del_Str2;
length store_type $7;
if EST_ACV >= 23.85 then store_type = "large";
else if EST_ACV > 6 and EST_ACV < 23.85 then store_type = "Medium";
else store_type = "small";
run;

/* import demographics dataset */
proc import datafile="F:\saltsnck\ads demo1.csv" out=snaks.demo dbms=csv replace;
getnames=yes;
run;

proc print data = snaks.demo(obs = 10);run;

data snaks.demo (drop = COUNTY HH_AGE HH_EDU HH_OCC MALE_SMOKE HISP_FLAG HISP_CAT HH_Head_Race__RACE2_ HH_Head_Race__RACE3_ Microwave_Owned_by_HH ZIPCODE FIPSCODE market_based_upon_zipcode IRI_Geography_Number EXT_FACT);
set snaks.demo;
run;

proc means data = snaks.demo nmiss;
var Number_of_TVs_Used_by_HH Number_of_TVs_Hooked_to_Cable;
run;

data snaks.demo (drop = Number_of_TVs_Used_by_HH Number_of_TVs_Hooked_to_Cable Panelist_Type Language);
set snaks.demo;
run;


/*-----------Merging Product and Grocery Panel Data--------------------------*/

proc sql;
create table snaks.prod_panel_gr as select * from snaks.panel_groc, snaks.prod_data where panel_groc.COLUPC1 = prod_data.UPC_Code;
/* 176015 rows and 19 columns */

proc print data = snaks.prod_panel_gr(obs = 20);run;

proc freq data = snaks.prod_panel_gr;
table PACKAGE FLAVOR_SCENT TYPE_OF_CUT;
run;

data snaks.prod_panel_gr(drop = COLUPC TYPE_OF_CUT);
set snaks.prod_panel_gr;
tot_cost = UNITS*DOLLARS;
run;

proc sql;
select cateogry,sum(UNITS) from snaks.prod_panel_gr group by cateogry;
/*(cateogry parent_comp sub_comp brand VOL_EQ PRODUCT_TYPE PACKAGE FLAVOR_SCENT Oz)*/

data prod_panel_gr;
set snaks.prod_panel_gr;
run;

proc print data = prod_panel_gr(obs = 20);run;
proc freq data = prod_panel_gr;table PACKAGE*Oz;run;
proc sql;
select PACKAGE,count(Units) from prod_panel_gr where brand LIKE "%STA%" group by PACKAGE;

data prod_panel_gr_oz;
set prod_panel_gr;
if PACKAGE = "BAG" OR PACKAGE = "CANISTER";
if Oz >= 0.6 AND Oz <= 1.2 then Pack_Type = "Small";
else if Oz >= 4.9 AND Oz <= 6.7 then Pack_Type = "Box";
else if Oz >= 9 AND Oz <= 11 then Pack_Type = "Regular";
else if Oz >= 12 AND Oz <= 15 then Pack_Type = "Party";
else Pack_Type = "NR";
run;

proc freq data = prod_panel_gr_oz;
table Pack_Type;
run;

proc sql;
select Pack_Type, cateogry, parent_comp, sub_comp, brand, SUM(UNITS) as TotUnits from prod_panel_gr_oz where Pack_Type <> "NR" GROUP BY Pack_Type, cateogry, parent_comp, sub_comp, brand having SUM(UNITS) > 2000;
run;

proc sql;
create table try1 as select PANID, Pack_Type, SUM(UNITS) as TotUnits from prod_panel_gr_oz where Pack_Type <> "NR" GROUP BY PANID, Pack_Type order by PANID, Pack_Type;

proc sql;
create table try3 as select PANID from try1 group by PANID having sum(TotUnits)>5;

proc sql;
create table try_mod as select try1.PANID, Pack_Type, TotUnits from try1, try3 where try1.PANID = try3.PANID;

proc means data = try1;
var TotUnits;
class Pack_Type;
run;

data try_mod_rank;
set try1;
if Pack_Type = "Party" then Party = TotUnits; 
else Party = 0;
if Pack_Type = "Small" then Small = TotUnits; 
else Small = 0;
if Pack_Type = "Regul" then Regular = TotUnits; 
else Regular = 0;
if Pack_Type = "Box" then Box = TotUnits; 
else Box = 0;
run;

data rank_try (drop = Pack_Type TotUnits);
set try_mod_rank;
run;

proc rank data = rank_try out = rank_prod_type group = 2 ties = low;
var Party Small Regular Box;
ranks Party_R Small_R Regular_R Box_R;
run;

proc contents data = rank_prod_type; run;

proc freq data = rank_prod_type; table Party_R Small_R Regular_R Box_R; run;

proc means data = rank_prod_type; var Party; class Party_R;run;

data snaks.rank_prod_type;
set rank_prod_type;
run;

proc export data = snaks.rank_prod_type outfile = "F:\saltsnck\rank.csv" dbms = csv replace;
run;

proc export data = snaks.demo outfile = "F:\saltsnck\demos_mod.csv" dbms = csv replace;
run;

proc sql;
create table merge_logit as select * from snaks.demo, snaks.rank_prod_type where demo.Panelist_ID = rank_prod_type.PANID;

proc print data = merge_logit_party(obs = 20);run;
/*Party*/
data merge_logit_party(drop = FEM_SMOKE Year PANID Party Small Regular Box Small_R Regular_R Box_R);
set merge_logit;
run;

proc logistic data = merge_logit_party descending out = logistic_out;
class Combined_Pre_Tax_Income_of_HH -- Marital_Status;
model Party_R = Combined_Pre_Tax_Income_of_HH -- Marital_Status;
run;

/*Small*/
data merge_logit_small(drop = FEM_SMOKE Year PANID Party Small Regular Box Regular_R Box_R Party_R);
set merge_logit;
run;

proc logistic data = merge_logit_small descending out = logistic_out_small;
class Combined_Pre_Tax_Income_of_HH -- Marital_Status;
model Small_R = Combined_Pre_Tax_Income_of_HH -- Marital_Status;
run;

/*Regular*/
data merge_logit_regular(drop = FEM_SMOKE Year PANID Party Small Regular Box Box_R Party_R Small_R);
set merge_logit;
run;

proc logistic data = merge_logit_regular descending out = logistic_out_small;
class Combined_Pre_Tax_Income_of_HH -- Marital_Status;
model Regular_R = Combined_Pre_Tax_Income_of_HH -- Marital_Status;
run;

/*Box*/
data merge_logit_box(drop = FEM_SMOKE Year PANID Party Small Regular Box Party_R Small_R Regular_R);
set merge_logit;
run;

proc logistic data = merge_logit_box descending out = logistic_out_box;
class Combined_Pre_Tax_Income_of_HH -- Marital_Status;
model Box_R = Combined_Pre_Tax_Income_of_HH -- Marital_Status;
run;

data merge_logit;
set snaks.merge_logit;
run;











