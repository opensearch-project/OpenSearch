
class Student {
    private String name;
    private int rollNo;

    public Student() {}

    public String getName() {
        return this.name;
    }

    public int getRoll() {
        return this.rollNo;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setRoll(int rollNo) {
        this.rollNo = rollNo;
    }
}

public class main {
    public static void main(String[] args) {
        Student s1 = new Student();
        s1.setName("Mridul");
        s1.setRoll(2107044);

        System.out.println("Name: " + s1.getName());
        System.out.println("Roll: " + s1.getRoll());
    }
}
