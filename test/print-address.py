import pymargo
from pymargo import MargoInstance

with MargoInstance('tcp') as mid: 
	print str(mid.addr())
