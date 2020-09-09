main:
	gcc -fPIC -shared -o mpitracer.so mpitracer.c -ldl -lpthread -O3 -Wall
