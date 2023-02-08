import matplotlib.pyplot as plt

# Ejemplo de diccionario
diccionario = {'A': 10, 'B': 20, 'C': 30}
# Obtener las claves y los valores
claves = list(diccionario.keys())
valores = list(diccionario.values())
# Crear un gráfico de barras
plt.bar(claves, valores)
# Mostrar el gráfico
plt.show()
