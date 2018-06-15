
import re

line = 'Cats are smarter than dogs'

matchObj = re.match(r'Cats' ,line)

print (matchObj.group())


matchObj = re.match(r'dogs' ,line)
if matchObj:
    (matchObj.group())
else:
    print ('Not match')

matchObj = re.search(r'Dogs' ,line, re.IGNORECASE)
print (matchObj.group())


phone = "925-997hhhl-2720 # This is Phone Number"

# Delete Python-style comments
num = re.sub(r'#.*$', "", phone)
print ("Phone Num : ", num)

# Remove anything other than digits
num = re.sub(r'\D', "", phone)
print ("Phone Num : ", num)


print ('xxxx:',phone[-3:1])



#!/usr/bin/python

# Function definition is here
def printinfo( arg1, *vartuple ):
   "This prints a variable passed arguments"
   print ("Output is: ")
   print (arg1)
   for var in vartuple:
      print (var)
   return;

# Now you can call printinfo function
#printinfo( 10 )
printinfo( 70, 60, 50 )




def multiply(*args):
    z = 1
    for num in args:
        print (num)
        z *= num
        print(num)

    print('*args',z)


list = [4,5]
multiply(4, 5)
multiply(10, 9)
multiply(2, 3, 4)
multiply(3, 5, 10, 6)


print ('Addition')
def addition(*args):
    z = 0
    for item in args:
        print('addition-0', z)
        z = z + item
        print('addition-1',z)

addition(1,2,3,4,5)






#You would use *args when you're not sure how many arguments might be passed to your function, i.e. it allows you pass an arbitrary number of arguments to your function. For example:

def print_everything(*args):

    for count, food in enumerate(args):
        print('{0}. {1},'.format(count, food))

print_everything('apple', 'banana', 'cabbage')

item = ['apple', 'banana', 'cabbage']
print_everything(*item)

#Similarly, **kwargs allows you to handle named arguments that you have not defined in advance:

def table_things(**kwargs):
    for name, value in kwargs.items():
        print( '{0} = {1}'.format(name, value))

table_things(apple = 'fruit', cabbage = 'vegetable')

table_things(apple = 'fruit', cabbage = 'vegetable', orange = 'fruit')

things = {'apple' : "fruit", 'cabbage' : "vegetable"}
table_things(**things)

things = {'apple' : "fruit", 'cabbage' : "vegetable",'organe': "fruit"}
table_things(**things)

# keyword Arguments
def parrot(voltage, state='a stiff', action='voom', type='Norwegian Blue'):
    print("-- This parrot wouldn't", action, end=' ')
    print("if you put", voltage, "volts through it.")
    print("-- Lovely plumage, the", type)
    print("-- It's", state, "!")


#lambda

f = lambda one, two, three :  one* two *three
f(2,3,6)
print (f(2,3,6))


#map
a = [1,2,3,4]
b = [17,12,11,10]
c = [-1,-4,5,9]
z = map(lambda x,y:x+y, a,b)

for item in z:
    print (item)


map(lambda x,y,z:x+y+z, a,b,c)

map(lambda x,y,z:x+y-z, a,b,c)

#filtering
fib = [0,1,1,2,3,5,8,13,21,34,55]
result = filter(lambda x: x % 2, fib) #% remainder
print (result)
#[1, 1, 3, 5, 13, 21, 55]

result = filter(lambda x: x % 2 == 0, fib)
print (result)
#[0, 2, 8, 34]

result = filter(lambda x: x * 2 >=10, fib)
print (result)
for item in result:
    print (item)

#reduce
f = lambda a,b: a if (a > b) else b
reduce(f, [47,11,42,102,13])
#102
print (r)
for item in r:
    print(item)
    str = 'Hello World!'
    print (str)
