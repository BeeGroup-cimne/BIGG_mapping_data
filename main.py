import happybase
# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

# AES 256 encryption/decryption using pycrypto library


def main():
    password = "3Ú3Tx9MYKYZMQ´qg"

    # First let us encrypt secret message
    message = input("FRANCESC CONTRERAS PASSWORD: ")
    encrypted = encrypt(message, password)
    print(encrypted)

    # Let us decrypt using our original password
    decrypted = decrypt(encrypted, password)
    print(bytes.decode(decrypted))


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
