#! /bin/sh
docker create -p 5601:5601 -p 9200:9200 -p 5044:5044 -p 5000:5000 -p 9300:9300 -it -v $(pwd):/home/$USER/ --name elk  sebp/elk
