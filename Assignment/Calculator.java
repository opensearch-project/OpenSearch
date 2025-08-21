public class Calculator {
    public static void main(String[] args) {

        System.out.println("5 + 5" + add(5, 5));
        System.out.println("5 - 5" + subtract(5, 5));
        System.out.println("5 * 5" + multiply(5, 5));
        System.out.println("5 / 5" + divide(5, 5));
    }
    
    public static float add(float num1, float num2) {
        return num1 + num2;
    }
    
    public static float subtract(float num1, float num2) {
        return num1 - num2;
    }
    
    public static float multiply(float num1, float num2) {
        return num1 * num2;
    }
    
    public static float divide(float num1, float num2) {
        if(num2 == 0) return Float.MAX_VALUE;
        return num1 / num2;
    }
}
