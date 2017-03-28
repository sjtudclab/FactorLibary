students=[('john', 'A', 15), ('jane', 'B', 12), ('dave', 'B', 10)]
#default ascending order
y = sorted(students, key=lambda student: student[2])
print (y,"\nAscending order by age: ",students)

import collections
Student = collections.namedtuple('Student', 'name grade age')
print ("Type of s is : ", type(Student))
s = Student(name = 'john', grade = 'A', age = 15)
print ("%s is %s years old, got an %s in Math" % (s.name, s.age, s.grade))
students=[Student(name = 'john', grade = 'C', age = 15), Student(name = 'Enna', grade = 'B', age = 12), Student(name = 'dave', grade = 'A', age = 10)]
print ("Descending order by age: ", sorted(students, key=lambda x: x.age, reverse = True))
print ("Ascending order by grade: ", sorted(students, key=lambda x: x.grade))

from operator import itemgetter, attrgetter
print ("Descending order by grade: ", sorted(students, key = itemgetter(1)))
print ("Ascending order by name: ", sorted(students, key = attrgetter('name')))
