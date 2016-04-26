package models

/**
 * Created by mourad.benabdelkerim on 4/26/16.
 */
case class Sale(
   saleId:Long,
   product:Int,
   shop:Int,
   clientId: Int,
   date:java.sql.Date,
   quantity:Int,
   price:Float,
   cashierId:Int,
   onOffer:Boolean
)