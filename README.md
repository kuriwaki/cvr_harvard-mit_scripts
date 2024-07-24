
<!-- README.md is generated from README.Rmd. Please edit that file -->

# Cast Vote Records: A Database of Ballots from the 2020 U.S. Election

##### Project leads: Shiro Kuriwaki and Mason Reece

<!-- badges: start -->

[![](https://img.shields.io/badge/Dataverse%20DOI-10.7910/DVN/PQQ3KV-blue)](https://www.doi.org/10.7910/DVN/PQQ3KV)
<!-- badges: end -->

This repository includes the code to create the cast vote record
dataset,

> Kuriwaki, Shiro; Reece, Mason; Baltz, Samuel; Conevska, Aleksandra;
> Loffredo, Joseph R.; Samarth, Taran; Mutlu, Can E.; Acevedo Jetter,
> Kevin E.; Garai, Zachary Djangoly; Murray, Kate; Hirano, Shigeo;
> Lewis, Jeffrey B.; Snyder, James M. Jr.; Stewart, Charles H. III,
> 2024, “Cast Vote Records: A Database of Ballots from the 2020 U.S.
> Election”, <https://doi.org/10.7910/DVN/PQQ3KV>, Harvard Dataverse.

The repository is limited to issue tracking and maintenance of data
construction scripts. It calls on a private Dropbox called `CVR_parquet`
for subsets of cleaned in parquet format.

`build.sh` is the root script for the entire project.

There is a data descriptor accompanying this dataset, with the following
abstract. Please access <https://www.doi.org/10.7910/DVN/PQQ3KV> for a
copy.

> Ballots are the basis of the electoral process. A growing group of
> political scientists, election administrators, and computer scientists
> have requested electronic records of actual ballots cast (cast vote
> records) from election officials, with the hope of affirming the
> legitimacy of elections and countering misinformation about ballot
> fraud. However, the administration of election data in the U.S. is
> scattered across local jurisdictions. Here we introduce a database of
> cast vote records from the 2020 U.S. general election. We downloaded,
> standardized, and extensively checked the accuracy of a set of cast
> vote records collected from the 2020 election. Our initial release
> includes six offices – President, Governor, U.S. Senate and House, and
> state upper and lower chambers – covering 40.9 million voters in 20
> states who voted for a total of thousands of candidates, including
> 2,121 Democratic and Republican candidates. This database serves as an
> unparalleled source of data for studying voting behavior and election
> administration.
