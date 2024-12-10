## Download Dataset

```sh
bash dataset/download.sh
```

## How to run?

### **Environtment**
```sh
docker-compose up -d
```

### **ELT**
```sh
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2 ./infra/elt/elt.py
```

### **Data Transfer**

```sh
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.5.0 ./infra/data-warehouse/data-transfer.py
```

### **Training**
```sh
py ./infra/ml-flow/training-script.py
```
