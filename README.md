![Header  Photo by Gracia Dharma on Unsplash](img/001.jpg)

# MAL reviews scraping :sparkles:
### :bar_chart: Proof of Concept 

Scripts for web scraping anime review data from https://myanimelist.net/ into a SQL database 

A sample ![Database](db/anime.db) is included in this repo.

## Executing

To run this code on your hardware
```bash
chmod u+x run.sh
./run.sh fresh    #Fresh/First time run
./run.sh refresh  #ReRun as Fresh saves the existing DB
./run.sh          #If you already have a DB 
```

#### ![LICENSE](LICENSE)
