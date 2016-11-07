#! /bin/sh
docker create -p 5601:5601 -p 9200:9200 -p 5044:5044 -p 5000:5000 -p 9300:9300 -it -v $(pwd):/home/$USER/ --name elk-2.4  sebp/elk:es240_l240_k460
docker create -p 5601:5601 -p 9200:9200 -p 5044:5044 -p 5000:5000 -p 9300:9300 -it -v $(pwd):/home/$USER/ --name elk-5.0  sebp/elk:es500_l500_k500
