package ydb

func Car(a int) string {
    switch {
    case a == 0:
        return "A"
    case a == 1:
        return "B"
    case a == 2:
        return "C"
    case a == 3:
        return "D"
    case a == 4:
        return "E"
    case a == 5:
        return "F"
    case a == 6:
        return "G"
    case a == 7:
        return "H"
    case a == 8:
        return "I"
    case a == 9:
        return "J"        
    }
    return "enormous"
}