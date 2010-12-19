$(function(){
	
	// setup
	$("#qlcontrol").text("Show queries ...");
	$("#dscontrol").text("Show datasets ...");

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
	
	$("#dscontrol").click(function () {
		if($("#datasets").is(":visible")){
			$("#datasets").slideUp("slow");
			$("#dscontrol").text("Show datasets ...");
		}
		else {
			$("#datasets").slideDown("slow");
			$("#dscontrol").text("Hide datasets ...");
		}
	});
});