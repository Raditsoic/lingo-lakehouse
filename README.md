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
spark-submit \ 
  --packages org.apache.hadoop:hadoop-aws:3.3.2 \
  ./infra/elt/elt.py
```
