select top 100 * from Orders

select top 200 * from OrderDetails

select top 200 * from Item

select top 200 * from ItemAggregate

select top 200 * from Cartitem

select top 200 * from Category

select top 200 * from Event

select top 200 * from [User]

select top 200 * from Orders a join OrderDetails b on a.OrderId = b.OrderId 
join Item c on b.ProductId = c.ItemId 
join Category d on c.CategoryId = d.CategoryId 
join [User] e on e.CategoryId = d.CategoryId
where a.OrderId = 97


SELECT [value] FROM OPENJSON(
  (SELECT
    id = o.OrderID,
    o.OrderDate,
    o.FirstName,
    o.LastName,
    o.Address,
    o.City,
    o.State,
    o.PostalCode,
    o.Country,
    o.Phone,
    o.Total,
    (select OrderDetailId, ProductId, UnitPrice, Quantity from OrderDetails od where od.OrderId = o.OrderId for json auto) as OrderDetails
   FROM Orders o FOR JSON PATH)
)


SELECT
  o.OrderID,
  o.OrderDate,
  o.FirstName,
  o.LastName,
  o.Address,
  o.City,
  o.State,
  o.PostalCode,
  o.Country,
  o.Phone,
  o.Total,
  (select OrderDetailId, ProductId, UnitPrice, Quantity from OrderDetails od where od.OrderId = o.OrderId for json auto) as OrderDetails
FROM Orders o;