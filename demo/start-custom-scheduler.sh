#!/bin/bash

kubectl uncordon master
kubectl create -f ../configs/custom-role.yaml
kubectl create -f ../configs/custom-scheduler.yaml
kubectl cordon master
