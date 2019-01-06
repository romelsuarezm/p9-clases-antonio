#Cambiar el valor ctic-227716 - Conetividad con JUPYTER
gcloud beta dataproc clusters create cluster-ctic --subnet default --zone us-west1-a --master-machine-type n1-standard-2 --master-boot-disk-size 300 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 200 --optional-components=ANACONDA,JUPYTER --image-version 1.3-deb9 --project ctic-227716
