# Tarea #1 - Big Data

## Objetivo

Introducir a los estudiantes al uso de operaciones de Apache Spark para la carga e integración de datos mediante el uso de `pytest`.

---

# Datos Generales

- La tarea debe ser implementada de forma individual o en grupos de máximo 3 personas.
- La fecha de entrega es el lunes 25 de mayo de 2026 antes de las 7:00 p. m.
- Cualquier indicio de copia será calificado con una nota de 0. La copia incluye código o configuraciones que puedan encontrarse en Internet y que sean utilizados parcial o totalmente sin el debido reconocimiento al autor.
- Se debe incluir documentación con instrucciones claras para ejecutar el proyecto.
- Se espera que las personas integrantes del grupo entiendan la implementación suministrada.
- El lenguaje de implementación es Python o Scala.
- El correo electrónico de entrega debe seguir el formato explicado durante la primera lección, además de incluir:
  - El enlace a una copia del código de la tarea en formato `.tar.gz` o `.zip`, almacenado en Google Drive o OneDrive.
  - Un enlace al repositorio donde se encuentra almacenado el código de la tarea.
  - Un enlace a un video de no más de 7 minutos donde se explique la solución desarrollada.
- Los grupos de trabajo deben autogestionarse; la persona facilitadora del curso no es responsable si algún miembro del equipo no realiza su trabajo.
- En consultas presenciales, por correo electrónico o por WhatsApp, como mínimo se debe haber leído sobre el tema e intentado comprenderlo. Las consultas deben ser concretas y se debe demostrar que se intentó resolver el problema en cuestión.
- Las herramientas de IA generativa pueden utilizarse como apoyo para el desarrollo de soluciones y la comprensión de problemas o errores que puedan presentarse. Sin embargo, todo código entregado para esta tarea debe ser desarrollado únicamente por las personas integrantes del grupo, quienes deberán entender en detalle lo que están entregando. El código generado por IA no puede presentarse total ni parcialmente como código de autoría de la persona estudiante. En caso de detectarse fraude, se obtendrá una nota de 0.
- El instructor de este curso se reserva el derecho de solicitar una defensa de la tarea de manera virtual mediante la plataforma Zoom.

---

# Resultados esperados

Para esta asignación se espera que los estudiantes concluyan dos entregables relacionados:

- Un programa principal que, dada la información sobre ciclistas, retorne el top 5 de ciclistas por provincia. El ranking debe realizarse según:
  - El total de kilómetros recorridos.
  - El promedio diario de kilómetros recorridos.
- Una serie de pruebas unitarias que permitan corroborar la correctitud de las diferentes funciones internas del programa.
- Ambos entregables deben ser suministrados como un directorio comprimido que incluya también la configuración de Docker necesaria para crear el contenedor y ejecutar exitosamente tanto las pruebas como el programa principal.

Las personas estudiantes serán responsables de documentar en su entregable cualquier instrucción necesaria para la correcta ejecución.

---

## Datos de entrada

Se asumirá la existencia de 3 entidades.

### 1. Ciclista

| Atributo | Tipo |
|---|---|
| Cédula | Numérico |
| Nombre completo | String |
| Provincia | String (San José, Alajuela, Cartago, etc.) |

### 2. Ruta

| Atributo | Tipo |
|---|---|
| Código de ruta | Identificador numérico |
| Nombre de ruta | String |
| Kilómetros | Numérico decimal |

### 3. Actividad

| Atributo | Tipo |
|---|---|
| Código de ruta | Numérico |
| Cédula | Numérico |
| Fecha | Formato YYYY-MM-DD |

Para la ejecución del programa principal, los estudiantes deberán proporcionar 3 archivos en formato CSV (separados por comas) con suficientes datos para ejemplificar la correcta funcionalidad de la solución.

Los archivos no deben incluir fila de encabezado y las columnas deben mantener exactamente el mismo orden indicado en cada uno de los apartados anteriores.

---

# Programa principal (20)

Se espera que los estudiantes entreguen un manual en formato PDF con las instrucciones necesarias para ejecutar el programa principal.

Idealmente, la ejecución debería realizarse mediante una simple llamada como la siguiente:

```bash
spark-submit programaestudiante.py ciclista.csv ruta.csv actividad.csv
```

Cualquier detalle adicional requerido para la correcta ejecución debe documentarse en dicho manual.

La imposibilidad de ejecutar el programa impedirá la obtención de los puntos correspondientes.

---

# Pruebas unitarias esperadas

Para la realización de las pruebas unitarias, se espera que los estudiantes identifiquen las diferentes partes necesarias para alcanzar el objetivo final.

Estas pruebas podrán partir de datos cargados en memoria, asumiendo que el código será suficientemente modular para que el programa principal únicamente invoque funciones reutilizables que puedan ser probadas mediante diferentes pruebas unitarias.

Los estudiantes deberán diseñar sus propias pruebas unitarias, utilizando la discusión en clase como base para guiar dicho diseño.

Para efectos de evaluación, se espera que existan suficientes pruebas para validar las diferentes áreas funcionales.

---

## Unión de los datos (20 puntos)

El primer paso debe consistir en unir los 3 conjuntos de datos diferentes.

Deberán existir funciones encargadas específicamente de esta responsabilidad.

Nótese que la unión de los datos no necesariamente es trivial. Por ejemplo:

- Es posible que se tenga el nombre de un ciclista que todavía no haya realizado ninguna actividad.
- Un mismo ciclista podría ejecutar la misma ruta múltiples veces durante un mismo día.

---

## Agregaciones parciales (35 puntos)

Con el objetivo final de encontrar los/las ciclistas con mayor cantidad de actividades, se solicita desarrollar código que permita crear dataframes intermedios donde se tenga:

- El total de kilómetros recorridos por persona.
- El total de kilómetros recorridos por provincia.
- El total de kilómetros recorridos por día.

Se espera que existan pruebas que partan de dataframes intermedios previamente construidos (es decir, que no requieran comenzar desde la unión de los datos) y que validen la correcta agregación de la información.

---

## Resultados finales (25 puntos)

Los estudiantes deberán crear pruebas que partan de las agregaciones parciales previamente construidas para retornar el top N de ciclistas por provincia, tanto según:

- El total de kilómetros recorridos.
- El promedio diario de kilómetros recorridos.

Las pruebas deben cubrir también casos excepcionales.

Tanto el profesor como el asistente se reservan el derecho de agregar pruebas unitarias adicionales en cada apartado con el fin de asegurar el correcto funcionamiento de la solución.

La nota será derivada completamente de las pruebas unitarias.

Deberá ser posible ejecutar las pruebas simplemente ejecutando el comando `pytest` dentro de la carpeta entregada con el código:

```bash
pytest
```
