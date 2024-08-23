# eth-video-indexer
Create an index of the entire video site of eth. Given username and password for ldap / for a specific series or lecture, indexer can also fetch the url of all video files available on the eth video servers.


# Branch Info
This branch is to capture the state when the `unique` constraint on the `sites` table was still `URL` only and not `URL, IS_VIDEO`.