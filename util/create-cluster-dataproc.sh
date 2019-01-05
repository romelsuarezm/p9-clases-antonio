gcloud dataproc clusters create cluster-cticName --subnet default --zone asia-northeast1-b --master-machine-type n1-standard-2 --master-boot-disk-size 300 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 200 --optional-components=ANACONDA,JUPYTER --image-version 1.3-deb9 --project demodata-227400