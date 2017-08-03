/*Try below code for data load and merge: Hope its helpful.*/

libname saltsnck "E:\MKT SAS\Project\";
options user=saltsnck;
run;
data saltsnck.DrSales;
infile 'E:\MKT SAS\Project\data\saltsnck_drug_1114_1165' firstobs=2 MISSOVER;
input IRI_KEY 1-7 WEEK 9-12 SY $ 14-15 GE $ 17-18 VEND $ 20-24 ITEM $ 26-30 UNITS 32-36 DOLLARS 38-45 F $ 47-50 D 52 PR 54;
if (length(trim(SY))=1) then SY='0'||trim(SY);
if (length(trim(GE))=1) then GE='0'||trim(GE);
do while(length(trim(VEND))<5);
      VEND='0'||trim(VEND);
end;
do while(length(trim(ITEM))<5);
      ITEM='0'||trim(ITEM);
end;
Outlet='DR';
UPC=trim(SY)||'-'||trim(GE) ||'-'|| trim(VEND) ||'-'|| trim(ITEM);
run;

data saltsnck.GrSales;
infile 'E:\MKT SAS\Project\data\saltsnck_groc_1114_1165' firstobs=2 MISSOVER;
input IRI_KEY 1-7 WEEK 9-12 SY $ 14-15 GE $ 17-18 VEND $ 20-24 ITEM $ 26-30 UNITS 32-36 DOLLARS 38-45 F $ 47-50 D 52 PR 54;
if (length(trim(SY))=1) then SY='0'||trim(SY);
if (length(trim(GE))=1) then GE='0'||trim(GE);
do while(length(trim(VEND))<5);
      VEND='0'||trim(VEND);
end;
do while(length(trim(ITEM))<5);
      ITEM='0'||trim(ITEM);
end;
Outlet='Gr';
UPC=trim(SY)||'-'||trim(GE) ||'-'|| trim(VEND) ||'-'|| trim(ITEM);
run;

proc import datafile="E:\MKT SAS\Project\data\prod_saltsnck" out=saltsnck.ProductDetails_orig dbms=xls replace;
getnames=yes;
run;


Data saltsnck.ProductDetails;
set saltsnck.ProductDetails_orig;
drop L1-L2 _STUBSPEC_1431RC;
COLUPC=compress(UPC,'-')*1;
run;
data saltsnck.Sales;
set saltsnck.DrSales  saltsnck.GrSales ;
run;

***delete temporary datasets;
/*
proc datasets lib=work  memtype=data nolist;
delete DrSales GrSales;
quit; */

proc sort data=saltsnck.ProductDetails;
by UPC;
quit;

proc sort data=saltsnck.sales;
by UPC;
quit;

data saltsnck.Prod_Sales (keep= IRI_KEY WEEK UNITS DOLLARS  F D PR Outlet L5 PRODUCT_TYPE UPC);
merge saltsnck.sales saltsnck.productdetails;
by UPC;
run;


data saltsnck.prod_sales;
set saltsnck.prod_sales;
Company = L3;
Brand = scan(L5,1);
if L5 = 'WAVY LAYS' then Brand = 'LAYS';
if L5 = 'FRITO LAY' then Brand = 'LAYS';
if L5 = 'BAKED LAYS' then Brand = 'LAYS';
if L5 = 'BAKED RUFFLES' then Brand = 'RUFFLES';
if L5 = 'BAKED TOSTITOS' then Brand = 'TOSTITOS';
/*PRICE = (DOLLARS/UNITS)/Vol_EQ;*/
/*size=scan(L9,-1," ");*/
Run;
/**/
/*PROC FREQ data=a.prod_sales;*/
/*TABLE Company brand size product_type TYPE_OF_CLEANER_DISF form flavor_scent */
/*strength additives CONCENTRATION_LEVEL F D PR OUTLET;*/
/*run;*/
/******** DELETE MISSING VALUES *************/


/******** DELETE MISSING VALUES *************/

data saltsnck.prod_sales;
set saltsnck.prod_sales;
IF PR='.' then DELETE;
run;

proc gchart data=saltsnck.prod_sales;
pie PRODUCT_TYPE/percent= inside;
run;


PROC MEANS DATA=saltsnck.prod_sales nmiss;run;


/************Top Sales Brand*************/
proc sql;
create table saltsnck.brand as
select  Brand, sum(dollars) as Dollars
from saltsnck.prod_sales
group by Brand
Order by Dollars DESC;

PROC PRINT data=saltsnck.brand(obs=10);
run;

PROC MEANS DATA=saltsnck.brand sum; var  dollars;run;

/**********************Panel Data**************************/

data p1;
infile 'E:\MKT SAS\Project\data\saltsnck_PANEL_GR_1114_1165.dat' firstobs=2 expandtabs;
input PANID	WEEK UNITS OUTLET $ DOLLARS IRI_KEY UPC :$17.;
run;

data p2;
infile 'E:\MKT SAS\Project\data\saltsnck_PANEL_DR_1114_1165.dat' firstobs=2 expandtabs;
input PANID	WEEK UNITS OUTLET $ DOLLARS IRI_KEY UPC :$17.;
run;

data p3;
infile 'E:\MKT SAS\Project\data\saltsnck_PANEL_MA_1114_1165.dat' firstobs=2 expandtabs;
input PANID	WEEK UNITS OUTLET $ DOLLARS IRI_KEY UPC :$17.;
run;


data saltsnck.panel (drop=l SY GE VEND ITEM);
set p1 p2 p3 ;

	UPC=TRIM(UPC);
	l=length(UPC);
	ITEM=substr(UPC, l-4,5);
	VEND=substr(UPC, l-9,5);
	GE=substr(UPC, l-10,1);
	if (length(trim(GE))=1) then GE='0'||trim(GE);
	if l gt 11 then 
		do;
			SY=substr(UPC, l,l-11);
			if (length(trim(SY))=1) then SY='0'||trim(SY);
		end;
	else
		SY='00';
	UPC=trim(SY)||'-'||trim(GE) ||'-'|| trim(VEND) ||'-'|| trim(ITEM);
run;
proc print data=saltsnck.panel(obs=5);run;


proc sort data=saltsnck.panel;
by UPC;
quit;
proc sort data=saltsnck.ProductDetails;
by UPC;
quit;

data saltsnck.panel_prod;
merge saltsnck.panel(in=pan) saltsnck.productdetails(in=prod);
by UPC;
if pan and prod;
run;
proc print data=saltsnck.panel_prod(obs=5);run;

data saltsnck.panel_prod(drop=l OZ);
set saltsnck.panel_prod;
Company = L3;
Brand = scan(L5,1);
if L5 = 'WAVY LAYS' then Brand = 'LAYS';
if L5 = 'FRITO LAY' then Brand = 'LAYS';
if L5 = 'BAKED LAYS' then Brand = 'LAYS';
if L5 = 'BAKED RUFFLES' then Brand = 'RUFFLES';
if L5 = 'BAKED TOSTITOS' then Brand = 'TOSTITOS';
	OZ=scan(L9,-1," ");
	l=length(OZ);
	OZ=substr(OZ, 1,l-2);
	OZ=trim(OZ);
	weight=OZ+0;
	PRICE = DOLLARS/(UNITS*weight);
Run;
proc print data=saltsnck.panel_prod(obs=5);var weight;run;

/************Top Sales Brand*************/
proc sql;
create table saltsnck.PanelBrand as
select  Brand, sum(dollars) as Dollars
from saltsnck.panel_prod
group by Brand
Order by Dollars DESC;

PROC PRINT data=saltsnck.PanelBrand(obs=10);
run;

/************************Merge Demo Data******************************/

/*Demo Data*/
PROC IMPORT OUT= saltsnck.demo 
            DATAFILE= "E:\MKT SAS\Project\data\ads demo1.CSV" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data saltsnck.demo;
set saltsnck.demo;
PanID = Panelist_ID +0;
drop panelist_ID;
run;

Proc Sort data=saltsnck.demo;
by PanID;
run;
Proc Sort data=saltsnck.panel_prod;
by PanID;
run;

/*Merge panel_prod and demo*/

data saltsnck.panel_prod_demo;
merge saltsnck.panel_prod(in=aa) saltsnck.demo(in=bb);
by panID;
if aa and bb;
run;
/* (keep=PanID week BR Brand units Combined_Pre_Tax_Income_of_HH Family_Size Type_of_Residential_Possession Children_Group_Code outlet PRICE IRI_key UPC)
*/
data saltsnck.panel_prod_demo_cleaned;
set saltsnck.panel_prod_demo;
if Brand = 'LAYS' then BR = 1;
if Brand = 'TOSTITOS' then BR = 2;
if Brand = 'WISE' then BR = 3;
if Brand = 'PRINGLES' then BR = 4;
run;
PROC PRINT data=saltsnck.panel_prod_demo_cleaned(obs=10);
run;

/************Top Sales Brand*************/
proc means data=saltsnck.panel_prod_demo;
	*var PRICE;
	class brand;
run;
proc sql;
create table temp as
select  Brand, count(Brand) as numb
from saltsnck.panel_prod_demo_cleaned
group by Brand
Order by numb DESC;

PROC PRINT data=temp(obs=10);
run;

data work.temp;
set saltsnck.panel_prod_demo;
if Brand = 'LAYS' then BR = 1;
if Brand = 'TOSTITOS' then BR = 2;
if Brand = 'WISE' then BR = 3;
if Brand = 'PRINGLES' then BR = 4;
run;
proc means data=work.temp mean;
	*var PRICE;
	class BR;
	output out=work.means;
run;

/* remove brands which are not in study */
data saltsnck.panel_prod_demo_cleaned;
set saltsnck.panel_prod_demo_cleaned;
IF BR='.' then DELETE;
run;

Proc means data=saltsnck.panel_prod_demo_cleaned;
class brand;
run;

/* START geting weekly mean price for four brands */
Proc means data=saltsnck.panel_prod_demo_cleaned mean NOPRINT;
class week BR;
var price;
output out=saltsnck.weekly_mean_price;
run;

data saltsnck.weekly_mean_price(drop=_STAT_ _TYPE_ _FREQ_);
set saltsnck.weekly_mean_price;
IF _STAT_='MEAN' && _TYPE_=3 then output;
run;

proc transpose data=saltsnck.weekly_mean_price
	out=saltsnck.weekly_mean_price;
	by week;
	id BR;
run;

data saltsnck.weekly_mean_price(drop=_1-_4 _NAME_);
	set saltsnck.weekly_mean_price;
	P1 = _1;
	P2 = _2;
	P3 = _3;
	P4 = _4;
run;

proc print data=saltsnck.weekly_mean_price (obs=10); run;
/* END geting weekly mean price for four brands */

/*Merge panel_prod_demo and avg price per week for brands */
Proc Sort data=saltsnck.panel_prod_demo_cleaned;
by week;
run;

data saltsnck.panel_prod_demo_cleaned;
merge saltsnck.panel_prod_demo_cleaned(in=aa) saltsnck.weekly_mean_price(in=bb);
by week;
if aa and bb;
run;

/* get the actual price back to its repective P? */
/*
data saltsnck.panel_prod_demo_cleaned;
	set saltsnck.panel_prod_demo_cleaned;
	array Pvec{4} P1 - P4;
	Pvec{BR}=price;
run; */

/******** merge sales data **************/
proc sort data=saltsnck.sales;
by IRI_key week UPC;
quit;
proc sort data=saltsnck.panel_prod_demo_cleaned;
by IRI_key week UPC;
quit;
data saltsnck.panel_prod_demo_cleaned;
merge saltsnck.panel_prod_demo_cleaned(in=pan) saltsnck.sales(in=prod);
by IRI_key week UPC;
if pan and prod;
run;
proc print data=saltsnck.panel_prod_demo_cleaned(obs=5);run;

/* START geting weekly store wise display score for brands */
Proc means data=saltsnck.panel_prod_demo_cleaned max NOPRINT;
	class IRI_key week BR;
	var D;
	output out=saltsnck.weekly_mean_display;
run;

data saltsnck.weekly_mean_display(drop=_STAT_ _TYPE_ _FREQ_);
	set saltsnck.weekly_mean_display;
	IF _STAT_='MAX' && _TYPE_=7 then output;
run;

proc transpose data=saltsnck.weekly_mean_display
	out=saltsnck.weekly_mean_display;
	by IRI_key week;
	id BR;
run;

data saltsnck.weekly_mean_display(drop=_1-_4 _NAME_);
	set saltsnck.weekly_mean_display;
	D1 = _1;
	D2 = _2;
	D3 = _3;
	D4 = _4;
run;

proc print data=saltsnck.weekly_mean_display (obs=10); run;
/* END geting weekly store wise display score for brands */

/*Merge panel_prod_demo and weekly store wise display score for brands */
Proc Sort data=saltsnck.panel_prod_demo_cleaned;
by IRI_key week;
run;

data saltsnck.panel_prod_demo_cleaned;
merge saltsnck.panel_prod_demo_cleaned(in=aa) saltsnck.weekly_mean_display(in=bb);
by IRI_key week;
if aa and bb;
run;

/* replace missing weekly store wise display score for brands to 0 */
data saltsnck.panel_prod_demo_cleaned(drop=i);
	set saltsnck.panel_prod_demo_cleaned;
	array Dvec{4} D1 - D4;
	do i = 1 to 4; 
		if Dvec{i}=. then Dvec{i} = 0;
	end;
run;

/* START geting weekly store wise Price Reduction flag: (1 if TPR is 5% or greater, 0 otherwise) for brands */
Proc means data=saltsnck.panel_prod_demo_cleaned max NOPRINT;
	class IRI_key week BR;
	var PR;
	output out=saltsnck.weekly_mean_PR;
run;

data saltsnck.weekly_mean_PR(drop=_STAT_ _TYPE_ _FREQ_);
	set saltsnck.weekly_mean_PR;
	IF _STAT_='MAX' && _TYPE_=7 then output;
run;

proc transpose data=saltsnck.weekly_mean_PR
	out=saltsnck.weekly_mean_PR;
	by IRI_key week;
	id BR;
run;

data saltsnck.weekly_mean_PR(drop=_1-_4 _NAME_);
	set saltsnck.weekly_mean_PR;
	PR1 = _1;
	PR2 = _2;
	PR3 = _3;
	PR4 = _4;
run;

proc print data=saltsnck.weekly_mean_PR (obs=10); run;
/* END geting weekly store wise Price Reduction flag: (1 if TPR is 5% or greater, 0 otherwise) for brands */

/*Merge panel_prod_demo and weekly store wise Price Reduction flag: (1 if TPR is 5% or greater, 0 otherwise) for brands */
Proc Sort data=saltsnck.panel_prod_demo_cleaned;
by IRI_key week;
run;

data saltsnck.panel_prod_demo_cleaned;
merge saltsnck.panel_prod_demo_cleaned(in=aa) saltsnck.weekly_mean_PR(in=bb);
by IRI_key week;
if aa and bb;
run;

/* replace missing weekly store wise Price Reduction flag: (1 if TPR is 5% or greater, 0 otherwise) for brands to 0 */
data saltsnck.panel_prod_demo_cleaned(drop=i);
	set saltsnck.panel_prod_demo_cleaned;
	array PRvec{4} PR1 - PR4;
	do i = 1 to 4; 
		if PRvec{i}=. then PRvec{i} = 0;
	end;
run;

/* formating Feature */
data panel_prod_demo_cleaned;
	set saltsnck.panel_prod_demo_cleaned;
	if F='NONE' then Feature = 0; else Feature=1;
run;

/* START geting weekly store wise Feature for brands */
Proc means data=panel_prod_demo_cleaned max NOPRINT;
	class IRI_key week BR;
	var PR;
	output out=saltsnck.weekly_mean_F;
run;

data saltsnck.weekly_mean_F(drop=_STAT_ _TYPE_ _FREQ_);
	set saltsnck.weekly_mean_F;
	IF _STAT_='MAX' && _TYPE_=7 then output;
run;

proc transpose data=saltsnck.weekly_mean_F
	out=saltsnck.weekly_mean_F;
	by IRI_key week;
	id BR;
run;

data saltsnck.weekly_mean_F(drop=_1-_4 _NAME_);
	set saltsnck.weekly_mean_F;
	F1 = _1;
	F2 = _2;
	F3 = _3;
	F4 = _4;
run;

proc print data=saltsnck.weekly_mean_F (obs=10); run;
/* END geting weekly store wise Feature for brands */

/*Merge panel_prod_demo and weekly store wise Feature for brands */
Proc Sort data=saltsnck.panel_prod_demo_cleaned;
by IRI_key week;
run;

data saltsnck.panel_prod_demo_cleaned;
merge saltsnck.panel_prod_demo_cleaned(in=aa) saltsnck.weekly_mean_F(in=bb);
by IRI_key week;
if aa and bb;
run;

/* replace missing weekly store wise Feature for brands to 0 */
data saltsnck.panel_prod_demo_cleaned(drop=i);
	set saltsnck.panel_prod_demo_cleaned;
	array Fvec{4} F1 - F4;
	do i = 1 to 4; 
		if Fvec{i}=. then Fvec{i} = 0;
	end;
run;

/* Creat indicator variable whether customer is owner on the household */
data saltsnck.panel_prod_demo_cleaned;
	set saltsnck.panel_prod_demo_cleaned;
	if Type_of_Residential_Possession=2 then HH_OWN = 1; else HH_OWN = 0;
run;

/* check FAT content */
proc freq data=saltsnck.panel_prod_demo_cleaned;
table brand*FAT_CONTENT / nocol nocum nopercent;
run;

proc freq data=saltsnck.panel_prod_demo_cleaned;
table brand*SALT_SODIUM_CONTENT / nocol nocum nopercent;
run;

/* final cleaning */
data temp(keep=PanID IRI_key week BR Brand units LOW_FAT Combined_Pre_Tax_Income_of_HH Family_Size 
HH_OWN Age_Group_Applied_to_Male_HH Education_Level_Reached_by_Male Occupation_Code_of_Male_HH 
Age_Group_Applied_to_Female_HH Education_Level_Reached_by_Femal Occupation_Code_of_Female_HH 
Children_Group_Code outlet P1-P4 D1-D4 F1-F4 PR1-PR4);
	set saltsnck.panel_prod_demo_cleaned;
	
	if FAT_CONTENT = 'MISSING' then LOW_FAT = 0; else LOW_FAT = 1;

	if Age_Group_Applied_to_Male_HH=7 then Age_Group_Applied_to_Male_HH = 0;
	if Education_Level_Reached_by_Male=9 then Education_Level_Reached_by_Male = 0;
	if Occupation_Code_of_Male_HH=11 then Occupation_Code_of_Male_HH = 13;
	if Occupation_Code_of_Male_HH=0 then Occupation_Code_of_Male_HH = 7;

	if Age_Group_Applied_to_Female_HH=7 then Age_Group_Applied_to_Female_HH = 0;
	if Education_Level_Reached_by_Femal=9 then Education_Level_Reached_by_Femal = 0;
	if Occupation_Code_of_Female_HH=11 then Occupation_Code_of_Female_HH = 13;
	if Occupation_Code_of_Female_HH=0 then Occupation_Code_of_Female_HH = 7;

	if Children_Group_Code=8 then Children_Group_Code = 0;
	if Children_Group_Code ne 0 then Children_Group_Code = 1;

run;

/* create in new data format */
data saltsnck.panel_prod_demo_cleaned_MNL(keep=pid decision mode Price Display Feature Price_Reduction 
dBR2 - dBR4 LOW_FAT2-LOW_FAT4 inc2-inc4 HH_OWN2-HH_OWN4 
Age_Male_HH2-Age_Male_HH4 Age_Male_EDU2-Age_Male_EDU4 Age_Male_OCC2-Age_Male_OCC4 
Age_Female_HH2-Age_Female_HH4 Age_Female_EDU2-Age_Female_EDU4 Age_Female_OCC2-Age_Female_OCC4 
nmemb2-nmemb4 kid2-kid4); 
   set temp; 

   array BRvec{3} dBR2 - dBR4 (0 0 0);
   array lFATvec{3} LOW_FAT2 - LOW_FAT4 (0 0 0);
   array INCvec{3} inc2 - inc4 (0 0 0);
   array hownvec{3} HH_OWN2 - HH_OWN4 (0 0 0);
   array Age_Male_HH{3} Age_Male_HH2 - Age_Male_HH4 (0 0 0);
   array Age_Male_EDU{3} Age_Male_EDU2 - Age_Male_EDU4 (0 0 0);
   array Age_Male_OCC{3} Age_Male_OCC2 - Age_Male_OCC4 (0 0 0);
   array Age_Female_HH{3} Age_Female_HH2 - Age_Female_HH4 (0 0 0);
   array Age_Female_EDU{3} Age_Female_EDU2 - Age_Female_EDU4 (0 0 0);
   array Age_Female_OCC{3} Age_Female_OCC2 - Age_Female_OCC4 (0 0 0);
   array nmembvec{3} nmemb2 - nmemb4 (0 0 0);
   array kidvec{3} kid2 - kid4 (0 0 0);

   array Pvec{4} P1 - P4;
   array Dvec{4} D1 - D4; 
   array Fvec{4} F1 - F4; 
   array PRvec{4} PR1 - PR4; 
   retain pid 0; 
   pid + 1; 
   do i = 1 to 4; 
      mode = i; 

      Price = Pvec{i}; 
	  Display = Dvec{i}; 
	  Feature = Fvec{i}; 
	  Price_Reduction = PRvec{i}; 
      decision = ( BR = i ); 
	  if i ne 1 then
	  	do j = 1 to 3;
	  		BRvec{j} = ((j+1) = mode);
			lFATvec{j} = BRvec{j} * LOW_FAT;
			INCvec{j} = BRvec{j} * Combined_Pre_Tax_Income_of_HH;
			hownvec{j} = BRvec{j} * HH_OWN;
			Age_Male_HH{j} = BRvec{j} * Age_Group_Applied_to_Male_HH;
			Age_Male_EDU{j} = BRvec{j} * Education_Level_Reached_by_Male;
			Age_Male_OCC{j} = BRvec{j} * Occupation_Code_of_Male_HH;
			Age_Female_HH{j} = BRvec{j} * Age_Group_Applied_to_Female_HH;
			Age_Female_EDU{j} = BRvec{j} * Education_Level_Reached_by_Femal;
			Age_Female_OCC{j} = BRvec{j} * Occupation_Code_of_Female_HH;
			nmembvec{j} = BRvec{j} * Family_Size;
			kidvec{j} = BRvec{j} * Children_Group_Code;
		end;
      output; 
	  BRvec{3} = 0;
	  lFATvec{3} = 0;
	  INCvec{3} = 0;
	  hownvec{3} = 0;
	  Age_Male_HH{3} = 0;
	  Age_Male_EDU{3} = 0;
	  Age_Male_OCC{3} = 0;
	  Age_Female_HH{3} = 0;
	  Age_Female_EDU{3} = 0;
	  Age_Female_OCC{3} = 0;
	  nmembvec{3} = 0;
	  kidvec{3} = 0;
   end; 
run; 
/*
PROc sort data=brand.NEWketchup;
	by PID decision;
RUN; */

proc mdc data=saltsnck.panel_prod_demo_cleaned_MNL;
   model decision = dBR2 - dBR4 Price Display Feature LOW_FAT2-LOW_FAT4 inc2-inc4 HH_OWN2-HH_OWN4 
Age_Male_HH2-Age_Male_HH4 Age_Male_EDU2-Age_Male_EDU4 Age_Male_OCC2-Age_Male_OCC4 
Age_Female_HH2-Age_Female_HH4 Age_Female_EDU2-Age_Female_EDU4 Age_Female_OCC2-Age_Female_OCC4 
nmemb2-nmemb4 kid2-kid4 /
            type=clogit
            nchoice=4;
   id pid;
   OUTPUT OUT = prob1 PRED=brandPROB ;
run;

proc sql;
create table predict as
select brandPROB, pid, decision
from prob1
order by pid, brandPROB desc;
run;
quit;

data predict;
set predict;
predict=0;
by pid;
if first.pid then predict=1;
run;

proc freq data=predict;
table predict*decision / nocol nocum nopercent;
run;

Proc means data=temp;
	*class BR;
run;
