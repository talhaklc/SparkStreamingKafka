public class SearchProductModel {
    private String product;
    private String time;

    public SearchProductModel(String product, String time) {
        this.product = product;
        this.time = time;
    }

    public SearchProductModel() {
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
