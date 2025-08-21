public class Grade {
    public static String getLetterGrade(float marks) {
        if (marks >= 80) return "A+";
        else if (marks >= 70) return "A";
        else if (marks >= 60) return "A-";
        else if (marks >= 50) return "B";
        else if (marks >= 40) return "C";
        else if (marks >= 33) return "D";
        else return "F";
    }

    public static void main(String[] args) {
        System.out.println("Marks: 85 → Grade: " + getLetterGrade(85));
        System.out.println("Marks: 62 → Grade: " + getLetterGrade(62));
        System.out.println("Marks: 28 → Grade: " + getLetterGrade(28));
    }
}
