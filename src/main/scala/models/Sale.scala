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
   onOffer:Boolean,
   col1: Int,
   col2: Int,
   col3: Int,
   col4: Int,
   col5: Int,
   col6: Int,
   col7: Int,
   col8: Int,
   col9: Int,
   col10: Int

)