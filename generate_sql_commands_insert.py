import random

# # List of some common Indian names
# indian_names = [
#     'Aarav', 'Vihaan', 'Vivaan', 'Ananya', 'Diya', 'Ishaan', 'Saanvi', 'Aadhya', 'Aditi', 'Ahana',
#     'Amit', 'Deepak', 'Gautam', 'Harshit', 'Ishan', 'Karan', 'Laksh', 'Mohit', 'Nikhil', 'Ojas',
#     'Pari', 'Prisha', 'Riya', 'Siya', 'Tanvi', 'Uma', 'Vedika', 'Yash', 'Zara', 'Bhavya',
#     'Chetan', 'Darsh', 'Eshaan', 'Falak', 'Gia', 'Himanshu', 'Ira', 'Jai', 'Kavya', 'Lavanya',
#     'Mihir', 'Naina', 'Oviya', 'Parth', 'Rahul', 'Sara', 'Tanish', 'Uday', 'Veer', 'Yuvraj'
# ]

# # Generate 500 INSERT INTO commands with random integers as keys and names from the list
# sql_commands = []
# for i in range(500):
#     name = random.choice(indian_names)
#     emp_id = random.randint(1, 1000)  # random integer as key, assuming keys can be duplicated
#     sql_command = f"INSERT INTO employee VALUES({emp_id}, '{name}');"
#     sql_commands.append(sql_command)


# for sql in sql_commands:
#     print(sql)
#     print("\n")

fileName = 'lognormal-190M.bin.data'
fileName = 'longitudes-200M.bin.data'

with open(fileName, mode='rb') as file: # b is important -> binary
    fileContent = file.read()
    print("Len of fileContent: ", len(fileContent))
    for item in fileContent[:190000000]:
        print("Type : ",type(item))
        print("Item : ",item)
    #print(fileContent)