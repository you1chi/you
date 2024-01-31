## LAB 7
## Jiayi Song

library(sqldf)
gss = read.csv('GSS.2006.csv')
g <- gss[ , c( "age","id", "region", "sibs", "childs", "marital", "wordsum", "educ", "realinc")]

## Questions ... ##


## 1. Find the age of person who has ID 1 in the dataframe (and show their ID too).  Provide the code to generate the results seen below. ##
## I did the first one for you... ##

sqldf("SELECT age, id FROM z 
WHERE id =1")