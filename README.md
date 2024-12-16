# Duolingo Learner Recommender System
Projek ini merupakan sebuah projek yang akan merekomendasikan language learning recommendation system agar bisa mengetahui bahasa dan/atau kata mana yang masih belum dipelajari oleh user dengan mencari user IDnya

## Anggota
| Nama       | NRP         | 
|------------|-------------|
| Wikri Cahya Syahrila  | 5027221020  |
| M Zidan Hadipratama     | 5027221052  |
| Awang Fraditya | 5027221055  |
| Marselinus Krisnawan R  | 5027221056  |
| Jonathan Adithya Baswara   | 5027221062  |


## Workflow
![workflow](Docummentation\Workflow.jpg)

# How The Program Works
## Download Dataset

```sh
bash dataset/download.sh
```
Untuk menjalankan program, kita mendownload dataset duolingo dengan menjalankan command di atas
[insert foto here]

## How to run?

### **Environtment**
```sh
docker-compose up -d
```
Jalankan environment docker dengan menjalankan command di atas
[Insert hasil foto here]

### **Start Consumer and Producer**
```sh
bash start.sh
```
Jalankan consumer dan producer dengan menjalankan command di atas
[insert foto here]

### **Apply Orchestration**
```sh
Go to http://localhost:8080, login and then apply Scripts
```
Berikut adalah tampilan UI setelah dijalankan
[Insert foto here]