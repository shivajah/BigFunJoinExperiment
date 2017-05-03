#import matplotlib.pyplot as plt
import pylab 
datalist = [('NBJ.txt','NBJ'),('hybrid.txt','Hybrid'),('NBJ50.txt','NBJ50')]
dt = [ ( pylab.loadtxt(filename), label ) for filename, label in datalist ]
for data, label in dt:
    pylab.plot( data[:,0], data[:,1], label=label, marker ='o' )
#pylab.plot([1,2,3,4], [1,2,3,4],'g', marker='o', label='NBJ')
#pylab.plot([2,7,3.5,9],'r', marker='*', label='NBJ50')
pylab.ylabel('Miliseconds')
pylab.xlabel('Memory in GB')
pylab.legend()
pylab.show()
