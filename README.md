# RayK8sDemo

#How to set up local dev env
1. Install docker(or similer container tool) https://docs.docker.com/get-started/introduction/get-docker-desktop/
1. Install minikube https://minikube.sigs.k8s.io/docs/start/?arch=%2Fmacos%2Farm64%2Fstable%2Fhomebrew 
1. Install Ray on minikube https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html#kuberay-rayjob-quickstart
   * Remember to provide large cpus and memory for minikube ```$ minikube start --cpus=8 --memory=4096```
