$(function(){
	
	// setup
	$("#qlcontrol").text("Queries ...");
	$("#impcontrol").text("Import ...");

	// events
	$("#saveq").click(function () {
		var qstrval = $("#querystr").val();
		// validate
		if(qstrval == "") {
			alert("Ah! You try to save an empty query. I don't like this ...");
			return false;
		}
		$.ajax({
			url : "/saveq",
			type: "POST",
			data: "querystr=" + qstrval,
			dataType: "text",
			success : function(data) {
				$("#result").html("Success! Query is saved ... " + data);
			},
			error: function(xhr, textStatus, errorThrown){
				alert("Error: " + textStatus);
			}
		});
	});
	
	$("#execq").click(function () {
		var qstrval = $("#querystr").val();
		// validate
		if(qstrval == "") {
			alert("Ah! You try to execute an empty query. I don't like this ...");
			return false;
		}
		$.ajax({
			url : "/execq",
			type: "GET",
			data: "querystr=" + qstrval,
			success : function(data) {
				$("#result").html("Success! Query is executed ... " + data);
			},
			error: function(xhr, textStatus, errorThrown){
				alert("Error: " + textStatus);
			}
		});
	});
		
	$("#qlcontrol").click(function () {
		if($("#querylist").is(":visible")){
			$("#querylist").slideUp("slow");
			$("#qlcontrol").text("Show queries ...");
		}
		else {
			$("#querylist").slideDown("slow");
			$("#qlcontrol").text("Hide queries ...");
		}
	});
	
	$("#impcontrol").click(function () {
		if($("#importf").is(":visible")){
			$("#importf").slideUp("slow");
			$("#impcontrol").text("Import ...");
		}
		else {
			$("#importf").slideDown("slow");
			$("#impcontrol").text("Hide import ...");
		}
	});
});