public class Driver {
    public static void main(String[] args) throws Exception {
        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();
        String transitMatrix = args[0];
        String prMatrix = args[1];
        String subPr = args[2];
        int count = Integer.parseInt(args[3]);
        for (int i = 0; i < count; i++) {
            String[] MultiplicationArgs = {transitMatrix, prMatrix + i, subPr + i};
            multiplication.main(MultiplicationArgs);
            String[] sumArgs = {subPr + i, prMatrix + (i + 1)};
            sum.main(sumArgs);
        }
    }
}
