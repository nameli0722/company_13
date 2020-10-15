
def same_animial():
    list_1 = ['cat', 'mouse', 'dog', 'horse', 'pig']
    list_2 = ['cat', 'cow', 'cock', 'dog']

    return (list_1 - list_1.difference(list_2))

same_animial()
