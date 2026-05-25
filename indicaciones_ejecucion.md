# Indicaciones para la ejecución de la Tarea 1

Instituto Tecnológico de Costa Rica Bimestre 3 2026
Escuela de Computación Módulo IV: Big Data
Programa Ciencia de Datos Asignación: Tarea 1

Profesor: Nereo Campos Araya 

Estudiantes:
- Fernanda Porras Mora 116940902
- Franco Quirós Montoya 304850621
- Héctor Saballos Zamora 207580427


### Indicaciones para la ejecución de la Tarea 1
Archivos necesarios para ejecutar el código
• actividad.csv
• build_image.sh
• ciclistas.csv
• conftest.py
• dockerfile
• functions.py
• main.py
• run_image.sh
• rutas.csv
• test_unit_tests.py
Instrucciones para la ejecución del código principal
1. Descargar y descomprimir el archivo comprimido (.zip) en un directorio local.
2. Abrir una terminal (command prompt) de la máquina local y buscar la dirección en la que  
se guardó la carpeta descargada.
3. Se debe renombrar dicha carpeta como “tarea_1” en el caso de que no sea este ya.
4. Se debe construir la imagen. Utilizar el comando:
```bash
docker build --tag bigdata .
```
(Nótese el punto al final del comando).
En caso de que no funcione, intentar con
```bash
bash build_image.sh
```
5. Se debe correr la imagen. Utilizar el comando:
```bash
docker run -p 8888:8888 -i -t bigdata /bin/bash
```
o en su defecto:
```bash
run_image.sh
```
6. Dentro del contenedor, se debe ejecutar el comando:
```bash
spark-submit main.py ciclistas.csv rutas.csv actividad.csv
```
Instrucciones para las pruebas unitarias
1. Dentro del contenedor, se debe ejecutar el comando:
```bash
pytest -s
```
Si se desea omitir el contenido de las pruebas, se debe utilizar:
```bash
pytest
```
