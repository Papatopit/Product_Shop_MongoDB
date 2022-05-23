import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Field;
import org.bson.Document;
import org.bson.BsonDocument;

import java.util.*;
import java.util.function.Consumer;

import static com.mongodb.client.model.Aggregates.*;


public class Main {

    private static MongoCollection<Document> collection_shop;
    private static MongoCollection<Document> collection_item;

    public static void main(String[] args) {

        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
        MongoDatabase database = mongoClient.getDatabase("localhost");

        // Создаем коллекцию
        collection_shop = database.getCollection("ShopListDemo");
        collection_item = database.getCollection("ItemListDemo");


        // Удалим из коллекций все документы
        collection_shop.drop();
        collection_item.drop();


        Scanner scanner = new Scanner(System.in);
        System.out.println(" Введите команду * help * для вывода справки! ");

        //считываем команду
        while (true) {
            String textIn = scanner.nextLine();

            String[] textSplitter = textIn.split("\\s");
            String command = textSplitter[0];

            if (command.equals("ADD_SHOP")) {           // Добавить магазин
                String shopName = textSplitter[1];
                addShop(shopName);
                continue;

            }
            if (command.equals("ADD_ITEM")) {          // Добавить товар
                String goodsName = textSplitter[1];
                int price = Integer.parseInt(textSplitter[2]);
                addItem(goodsName, price);
                continue;
            }
            if (command.equals("SET_ITEM")) {         //
                String goodsName = textSplitter[1];
                String shopName = textSplitter[2];
                setItem(goodsName, shopName);
                continue;
            }

            if (command.equals("STATISTICS")) {
                showStatistics();
                continue;
            }
            if (command.equals("end")) {
                break;
            }

            if (command.equals("help")) {
                printInstructions();

            } else {
                System.out.println(" Команда не распознана! ");
                printInstructions();
                break;
            }
        }
    }

    public static void printInstructions() {
        System.out.println("— Команда добавления магазина\n* ADD_SHOP имя_магазина - сначала идет название команды, а потом имя магазина, всегда одно слово, без пробелов");
        System.out.println("Например: ADD_SHOP Девяточка\n");

        System.out.println("— Команда добавления товара\n* ADD_ITEM наименование_товара стоимость - сначала идет название команды, а потом название товара, всегда одно слово, " +
                "без пробелов, затем целое число — цена товара в рублях");
        System.out.println("Например: ADD_ITEM Вафли 54\n");

        System.out.println("— Команда добавления товара в магазин\n* SET_ITEM наименование_товара имя_магазина - сначала идет название команды, а потом название товара, затем название магазина");
        System.out.println("Например SET_ITEM Вафли Девяточка\n");

        System.out.println("— Команда получения информации о товарах во всех магазинах\n" +
                "STATISTICS");

        System.out.println("\n— Выход\n" +
                "end\n\n");
    }


    public static void addShop(String shopName) {
        ArrayList<String> itemList = new ArrayList<>();
        Document shopDocument = new Document()
                .append("name", shopName)
                .append("itemList", itemList);

        if ((collection_shop.find(BsonDocument.parse("{name: {$eq: \"" + shopName + "\"}}")).first() == null)) {
            collection_shop.insertOne(shopDocument);
        } else {
            System.out.println("Магазин " + shopName + " уже имеется. Придумайте новое название магазина");
        }
    }


    public static void addItem(String itemName, int priceItem) {
        Document itemDocument = new Document()
                .append("name", itemName)
                .append("price", priceItem);

        if ((collection_item.find(BsonDocument.parse("{name: {$eq: \"" + itemName + "\"}}")).first() == null)) {
            collection_item.insertOne(itemDocument);
        } else {
            System.out.println("Товар " + itemName + " уже имеется. Придумайте новый товар");
        }
    }

    public static void setItem(String itemName, String shopName) {
        BsonDocument queryItem = BsonDocument.parse("{name: {$eq: \"" + itemName + "\"}}");
        BsonDocument queryShop = BsonDocument.parse("{name: {$eq: \"" + shopName + "\"}}");
        try {
            if (!(collection_item.find(queryItem).first() == null) && !(collection_shop.find(queryShop).first() == null)) {
                if (!((collection_shop.find(queryShop).first().getList("itemList", String.class).size() == 0))) {
                    collection_shop.findOneAndUpdate(queryShop, BsonDocument.parse("{$addToSet: {itemList: \"" + itemName + "\"}}"));
                } else {
                    collection_shop.findOneAndUpdate(queryShop, BsonDocument.parse("{$set: {itemList: [\"" + itemName + "\"]}}"));
                }
            } else {
                System.out.println("магазин: " + collection_shop.find(queryShop).first() + ", товар: " + collection_item.find(queryItem).first());
                System.out.println("Товар или магазин не найден");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void showStatistics() {
        // Общее количество товаров
        System.out.println("\n\nОбщее количество товаров в каждом магазине\n");

        collection_shop.aggregate(Arrays.asList(unwind("$itemList"), sortByCount("$name"))).forEach((Consumer<Document>) document -> {
            System.out.println("Наименование магазина: " + document.get("_id") + " - количество товара: " + document.get("count"));
        });

        // Среднюю цену товара
        System.out.println("\n\nСредняя цена товара в каждом магазине:\n");

        collection_shop.aggregate(Arrays.asList(
                lookup("ItemListDemo", "itemList", "name", "priceItem"),
                unwind("$priceItem"),
                group("$name", Accumulators.avg("avgPriceItem", "$priceItem.price"))
        )).forEach((Consumer<Document>) document -> System.out.println("Наименование магазина: " + document.get("_id") + " - Средняя цена товара: " + document.get("avgPriceItem")));


        // Самый дорогой и самый дешевый товар
        System.out.println("\n\nСамый дорогой и самый дешевый товар в каждом магазине:\n");

        collection_shop.aggregate(Arrays.asList(
                lookup("ItemListDemo", "itemList", "name", "priceItem"),
                unwind("$priceItem"),
                sort(new Document("name", 1)
                        .append("priceItem.price", 1)),
                group("$name", Arrays.asList(
                        Accumulators.first("minNameItem", "$priceItem.name"),
                        Accumulators.last("maxNameItem", "$priceItem.name"),
                        Accumulators.min("minPriceItem", "$priceItem.price"),
                        Accumulators.max("maxPriceItem", "$priceItem.price")
                ))
        )).forEach((Consumer<Document>) document ->
                System.out.println("Наименование магазина: " + document.get("_id") + "\n\t " +
                        "- Cамый дешевый товар: " + document.get("minNameItem") + " - " + document.get("minPriceItem") + " руб." + "\n\t " +
                        "- Cамый дорогой товар: " + document.get("maxNameItem") + " - " + document.get("maxPriceItem") + " руб."));


        // Количество товаров, дешевле 100 рублей.
        System.out.println("\n\nКоличество товаров, дешевле 100 рублей в каждом магазине:\n");


        collection_shop.aggregate(Arrays.asList(
                lookup("ItemListDemo", "itemList", "name", "priceItem"),
                unwind("$priceItem"),
                match(new Document("priceItem.price", new Document("$lt", 100))),
                addFields(new Field<>("count", 1)),
                group("$name", Accumulators.sum("count", "$count"))
        )).forEach((Consumer<Document>) document ->
                System.out.println("Наименование магазина: " + document.get("_id") + "\n\t " +
                        "- Количество товаров, дешевле 100 рублей: " + document.get("count")));
    }

}