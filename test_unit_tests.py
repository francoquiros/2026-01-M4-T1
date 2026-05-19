from functions import join_dfs, aggregate, top_n

# Unit tests para el JOIN

# TEST 1
def test_join_pass(spark_session):
        print("Unit Test: join pass")
        ciclistas = [(207580425, 'Alonso Arguedas', 'Heredia'),
                     (209190484, 'Elias Herrera', 'Alajuela'),
                     (408470591, 'Marco Jimenez', 'Heredia'),
                     (301780416, 'Javier Hernandez', 'Cartago'),
                     (118740876, 'Gabriel Vargas', 'San Jose'),
                     (305840417, 'Fabián Solera', 'Cartago'),
                     (601980756, 'Josue Granados', 'Puntarenas')]
        rutas = [()]