package ai.sapper.cdc.common.utils;

public class CompareUtils {
    public static int compare(double v1, double v2) {
        double r = v1 - v2;
        if (r < 1 && r > 0) {
            return 1;
        } else if (r > -1 && r < 0) {
            return -1;
        }
        return (int) r;
    }
    public static int compare(float v1, float v2) {
        float r = v1 - v2;
        if (r < 1 && r > 0) {
            return 1;
        } else if (r > -1 && r < 0) {
            return -1;
        }
        return (int) r;
    }

}
